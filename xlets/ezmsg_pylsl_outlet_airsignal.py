from collections.abc import AsyncGenerator
import os
import sys
from typing import Any
from pathlib import Path
from dataclasses import dataclass

import pylsl
import click
import ezmsg.core as ez
from ezmsg.util.messages.axisarray import AxisArray
from ezmsg.blackrock.nsp import NSPSource, NSPSourceSettings


@dataclass
class Message:
    sample: float
    timestamp: float


# ==================================================================
class NSPExtractorSettings(ez.Settings):
    tc: int


class NSPExtractorState(ez.State):
    current_count: int


class NSPExtractor(ez.Unit):
    SETTINGS = NSPExtractorSettings
    STATE = NSPExtractorState

    INPUT = ez.InputStream(AxisArray)
    OUTPUT = ez.InputStream(Message)

    async def extract(self, message: AxisArray) -> AsyncGenerator:
        if self.STATE.current_count >= self.SETTINGS.tc:
            yield (
                self.OUTPUT,
                Message(sample=-1, timestamp=pylsl.local_clock()),
            )
            raise ez.Complete
        else:
            sample = message.data[:, 1].item()
            yield (
                self.OUTPUT,
                Message(sample=sample, timestamp=pylsl.local_clock()),
            )
            self.STATE.current_count += 1


# ==================================================================
class LSLOutletSettings(ez.Settings):
    fs: int
    stream_name: str


class LSLOutletState(ez.State):
    outlet: Any


class LSLOutletUnit(ez.Unit):
    STATE = LSLOutletState
    SETTINGS = LSLOutletSettings

    INPUT = ez.InputStream(Any)

    def initialize(self) -> None:
        info = pylsl.StreamInfo(
            name=self.SETTINGS.stream_name,
            type=self.SETTINGS.stream_name,
            channel_count=1,
            nominal_srate=self.SETTINGS.fs,
        )

        self.STATE.outlet = pylsl.StreamOutlet(info=info, chunk_size=0)

    @ez.subscriber(INPUT)
    async def outlet(self, message: Message) -> None:
        sample, timestamp = message.sample, message.timestamp
        self.STATE.outlet.push_sample([sample], timestamp)

        if sample == -1:
            raise ez.Complete


# ==================================================================


class LogSettings(ez.Settings):
    log_file_name: Path


class LogState(ez.State):
    file: Any


class LogUnit(ez.Unit):
    SETTINGS = LogSettings
    STATE = LogState
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
        sample, timestamp = message.sample, message.timestamp

        print(timestamp, sample)

        if sample == -1:
            print("closing outlet and writing logs to disk...")
            self.STATE.file.flush()
            self.STATE.file.close()

            raise ez.Complete

        else:
            self.STATE.file.write(f"{timestamp},{sample}\n")


# ==================================================================


class AirsignalSystemSettings(ez.Settings):
    total_count: int
    fs: int
    multiproc: bool
    log_file_name: Path
    stream_name: str


class AirsignalSystem(ez.Collection):
    SETTINGS = AirsignalSystemSettings

    NSP = NSPSource()
    EXT = NSPExtractor()
    OUTLET = LSLOutletUnit()
    LOG = LogUnit()

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
        self.OUTLET.apply_settings(
            (
                LSLOutletSettings(
                    fs=self.SETTINGS.fs, stream_name=self.SETTINGS.stream_name
                )
            )
        )
        self.LOG.apply_settings(LogSettings(log_file_name=self.SETTINGS.log_file_name))

    def network(self) -> ez.NetworkDefinition:
        return (
            (self.NSP.OUTPUT_SIGNAL, self.EXT.INPUT),
            (self.EXT.OUTPUT, self.OUTLET.INPUT),
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
    if not verbose:
        sys.stdout = open(os.devnull if os.name != "nt" else "nul", "w")

    file_name = Path(
        f"./logs/id-{id}_outlet-ezmsgpylsl_datatype-{datatype}_platform-{platform}_multiproc-{str(mp)}_fs-{fs}_window-{ws}.csv"
    )
    click.echo(f"Logs: {file_name}")

    settings = AirsignalSystemSettings(
        total_count=tc,
        fs=fs,
        multiproc=mp,
        log_file_name=file_name,
        stream_name=datatype,
    )
    system = AirsignalSystem(settings)
    ez.run({"system": system})


if __name__ == "__main__":
    main()
