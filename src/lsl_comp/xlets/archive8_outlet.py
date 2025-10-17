import sys
import logging
from typing import Any
from pathlib import Path
from collections.abc import AsyncGenerator

import click
import ezmsg.core as ez
from ezmsg.util.messages.axisarray import AxisArray
from ezmsg.blackrock.nsp import NSPSource, NSPSourceSettings

from lsl_comp.ez_utils.message import Message
from lsl_comp.utils.pylogger import logger_creator

# ==================================================================


class LogOutletSettings(ez.Settings):
    log_file_name: Path
    logger: logging.Logger


class LogOutletState(ez.State):
    file: Any


class LogOutletUnit(ez.Unit):
    SETTINGS = LogOutletSettings
    STATE = LogOutletState
    INPUT = ez.InputStream(Any)

    def initialize(self) -> None:
        self.STATE.file = open(self.SETTINGS.log_file_name, "w")
        self.STATE.file.write(
            ",".join(
                [
                    "t_gen_outlet",
                    "x\n",
                ]
            )
        )

    @ez.subscriber(INPUT)
    async def on_message(self, message: Message) -> None:
        samples, timestamps = message.samples, message.timestamps

        self.SETTINGS.logger.debug((timestamps, samples))

        # if samples == [-1.0]:
        #     self.SETTINGS.logger.info("Writing logs to disk...")
        #     self.STATE.file.flush()
        #     self.STATE.file.close()
        #
        #     raise ez.Complete
        #
        # else:
        log_line = f"{';'.join([str(t) for t in timestamps])},{';'.join(str(s) for s in samples)}\n"
        self.STATE.file.write(log_line)
        self.STATE.file.flush()


# ==================================================================


class NSPExtractorSettings(ez.Settings):
    tc: int


class NSPExtractorState(ez.State):
    current_count: int


class NSPExtractorUnit(ez.Unit):
    SETTINGS = NSPExtractorSettings
    STATE = NSPExtractorState

    INPUT = ez.InputStream(AxisArray)
    OUTPUT = ez.OutputStream(Message)

    def initialize(self) -> None:
        self.STATE.current_count = 0

    @ez.subscriber(INPUT)
    @ez.publisher(OUTPUT)
    async def extract(self, message: AxisArray) -> AsyncGenerator:
        # NSPSource() returns a numpy array of shape (time, channels)

        # if self.STATE.current_count >= self.SETTINGS.tc:
        #     yield (
        #         self.OUTPUT,
        #         Message(samples=[-1.0], timestamps=[99.99]),
        #     )
        #
        #     raise ez.Complete
        #
        # else:
        # extract values from 2nd channel (index = 1) for all timesteps
        samples: list[float] = message.data[:, 1].tolist()

        # how many samples were actually received in a single message
        num_samples = len(samples)

        # extract timestamps received from hardware
        # WARN: maybe we should pylsl.local_clock() as the first timestamp
        # instead of reading it from the machine so it can be related back to markers timestamps
        gain: float = message.axes["time"].gain
        first_timestamp: float = message.axes["time"].offset.item()
        timestamps = [(first_timestamp + i * gain) for i in range(num_samples)]

        yield (
            self.OUTPUT,
            Message(samples=samples, timestamps=timestamps),
        )

        self.STATE.current_count += num_samples


# ==================================================================


class SystemSettings(ez.Settings):
    total_count: int
    fs: int
    multiproc: bool
    log_file_name: Path
    stream_name: str
    logger: logging.Logger


# ==================================================================


class AirsignalSystem(ez.Collection):
    SETTINGS = SystemSettings

    NSP = NSPSource()
    EXT = NSPExtractorUnit()
    OUTLET = LSLOutletUnit()
    LOG = LogOutletUnit()

    def configure(self) -> None:
        self.NSP.apply_settings(
            NSPSourceSettings(
                inst_addr="192.168.137.128",
                inst_port=51001,
                client_addr="",
                client_port=51002,
                recv_bufsize=(8 if sys.platform == "win32" else 6) * 1024 * 1024,
                protocol="4.1",
                cont_buffer_dur=0.5,
                microvolts=True,
                cbtime=False,
            )
        )
        self.EXT.apply_settings(NSPExtractorSettings(tc=self.SETTINGS.total_count))

        self.OUTLET.apply_settings(
            (
                LSLOutletSettings(
                    stream_name=self.SETTINGS.stream_name,
                    stream_type=self.SETTINGS.stream_name,
                )
            )
        )
        self.LOG.apply_settings(
            LogOutletSettings(
                log_file_name=self.SETTINGS.log_file_name, logger=self.SETTINGS.logger
            )
        )

    def network(self) -> ez.NetworkDefinition:
        return (
            (self.NSP.OUTPUT_SIGNAL, self.OUTLET.INPUT_SIGNAL),
            (self.NSP.OUTPUT_SIGNAL, self.EXT.INPUT),
            (self.EXT.OUTPUT, self.LOG.INPUT),
        )

    def process_components(self) -> tuple[ez.Component, ...]:
        if self.SETTINGS.multiproc:
            return (self.NSP, self.OUTLET, self.EXT, self.LOG)
        else:
            return ()


# ==================================================================


@click.command()
@click.option("--tc", type=click.INT, help="Total count.", required=True)
@click.option("--fs", type=click.INT, help="Sampling rate.", required=True)
@click.option("--mp", type=click.BOOL, help="Multiprocessing.", required=True)
@click.option("--ws", type=click.INT, help="Inlet window size.", required=True)
@click.option(
    "--datatype", type=click.STRING, help="counter, airsignal.", required=True
)
@click.option("--platform", type=click.STRING, help="Platform (os).", required=True)
@click.option("--verbose", type=click.BOOL, help="Verbosity.", default=True)
@click.option(
    "--id", type=click.INT, help="Run ID to pair inlet and outlet.", required=True
)
def main(
    tc: int,
    fs: int,
    mp: bool,
    ws: int,
    datatype: str,
    platform: str,
    verbose: bool,
    id: int,
):
    logger = logger_creator(verbose)

    file_name = Path(
        f"./logs/id-{id}_outlet-archive8_datatype-{datatype}_platform-{platform}_multiproc-{str(mp)}_fs-{fs}_window-{ws}.csv"
    )
    click.echo(f"Logs: {file_name}")

    settings = SystemSettings(
        total_count=tc,
        fs=fs,
        multiproc=mp,
        log_file_name=file_name,
        stream_name=datatype,
        logger=logger,
    )

    if datatype == "airsignal":
        system = AirsignalSystem(settings)
    else:
        raise ValueError("Incompatible datatype.")

    ez.run({"system": system})


if __name__ == "__main__":
    main()
