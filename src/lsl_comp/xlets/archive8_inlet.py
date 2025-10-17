import logging
from typing import Any
from pathlib import Path
from collections import deque
from collections.abc import AsyncGenerator

import click
import pylsl
import ezmsg.core as ez

from lsl_comp.utils.pylogger import logger_creator

# ==================================================================


class LogInletSettings(ez.Settings):
    window_size: int
    log_file_name: Path
    logger: logging.Logger


class LogInletState(ez.State):
    file: Any


class LogInletUnit(ez.Unit):
    STATE = LogInletState
    SETTINGS = LogInletSettings
    INPUT = ez.InputStream(str)

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
    async def on_message(self, message: str) -> None:
        # print(message)
        # if message == "-1.0":
        #     self.SETTINGS.logger.info("Writing logs to disk...")
        #     self.STATE.file.flush()
        #     self.STATE.file.close()
        #
        #     raise ez.Complete
        # else:
        self.STATE.file.write(message)
        self.STATE.file.flush()


# ==================================================================
class LSLInletSettings(ez.Settings):
    fs: int
    window_size: int
    stream_name: str
    logger: logging.Logger


class LSLInletState(ez.State):
    inlet: Any
    buffer: deque


class LSLInletUnit(ez.Unit):
    STATE = LSLInletState
    SETTINGS = LSLInletSettings

    OUTPUT = ez.OutputStream(str)

    def initialize(self) -> None:
        streams = pylsl.resolve_byprop("name", self.SETTINGS.stream_name)
        # self.STATE.inlet = pylsl.StreamInlet(streams[0], max_buflen=1)
        self.STATE.inlet = pylsl.StreamInlet(
            streams[0], processing_flags=pylsl.proc_ALL
        )
        self.STATE.buffer = deque(maxlen=self.SETTINGS.window_size)

    def buffer_print(self, buffer, logger) -> None:
        logger.debug((len(buffer), f"{buffer[0][-1]}...{buffer[-1][-1]}"))

    def buffer_to_string(self, buffer) -> str:
        log_line = [";".join((str(e) for e in b)) for b in list(zip(*buffer))]
        log_line = ",".join(log_line) + "\n"

        return log_line

    @ez.publisher(OUTPUT)
    async def inlet(self) -> AsyncGenerator:
        while True:
            # NOTE: archive 8
            # no windowing
            # sample, t_outlet = self.STATE.inlet.pull_sample()
            # self.SETTINGS.logger.debug((t_outlet, sample))
            #
            # if t_outlet:
            #     log_line = f"{t_outlet},{sample[0]}\n"
            #
            #     yield (self.OUTPUT, log_line)

            # NOTE: chunk is multiple timesteps with all channels: list[list[float]]
            chunk, t_outlet = self.STATE.inlet.pull_chunk()
            self.SETTINGS.logger.debug((t_outlet, chunk))

            if chunk and t_outlet:
                log_line = f"{t_outlet},{';'.join(chunk[0][0])}\n"

                yield (self.OUTPUT, log_line)

            # NOTE: archive 8
            # windowing
            # sample, t_outlet = self.STATE.inlet.pull_sample()
            # self.SETTINGS.logger.debug((t_outlet, sample))
            # if t_outlet:
            #     self.STATE.buffer.append((t_outlet, sample[0]))
            #
            #     if len(self.STATE.buffer) == self.SETTINGS.window_size:
            #         buffer_list = list(self.STATE.buffer)
            #         log_line = [f"{t},{s}\n" for t, s in buffer_list]
            #         yield (self.OUTPUT, log_line)
            #         self.STATE.buffer.clear()

            # NOTE: sample is a single timestep with all channels: list[float]
            # sample, t_outlet = self.STATE.inlet.pull_sample()
            # self.SETTINGS.logger.debug((t_outlet, sample))
            # if sample and t_outlet:
            #     log_line = f"{t_outlet},{';'.join(sample)}\n"
            #
            #     yield (self.OUTPUT, log_line)

            # NOTE: chunk is multiple timesteps with all channels: list[list[float]]
            # chunk, t_outlet = self.STATE.inlet.pull_chunk()
            # self.SETTINGS.logger.debug((t_outlet, chunk))
            #
            # if chunk and t_outlet:
            #     # get 0th channel from all timesteps in the message
            #     sample = [c[0] for c in chunk]
            #
            #     log_line = f"{t_outlet},{';'.join(sample)}\n"
            #
            #     yield (self.OUTPUT, log_line)


# ==================================================================


class SystemSettings(ez.Settings):
    fs: int
    window_size: int
    multiproc: bool
    log_file_name: Path
    stream_name: str
    logger: logging.Logger


class System(ez.Collection):
    SETTINGS = SystemSettings

    INLET = LSLInletUnit()
    LOG = LogInletUnit()

    def configure(self) -> None:
        self.INLET.apply_settings(
            (
                LSLInletSettings(
                    fs=self.SETTINGS.fs,
                    window_size=self.SETTINGS.window_size,
                    stream_name=self.SETTINGS.stream_name,
                    logger=self.SETTINGS.logger,
                )
            )
        )

        self.LOG.apply_settings(
            LogInletSettings(
                log_file_name=self.SETTINGS.log_file_name,
                window_size=self.SETTINGS.window_size,
                logger=self.SETTINGS.logger,
            )
        )

    def network(self) -> ez.NetworkDefinition:
        return ((self.INLET.OUTPUT, self.LOG.INPUT),)

    def process_components(self) -> tuple[ez.Component, ...]:
        if self.SETTINGS.multiproc:
            return (self.INLET, self.LOG)
        else:
            return ()


@click.command()
@click.option("--fs", type=click.INT, help="Sampling rate.", required=True)
@click.option("--mp", type=click.BOOL, help="Multiprocessing.", required=True)
@click.option("--ws", type=click.INT, help="Window size.", required=True)
@click.option(
    "--datatype", type=click.STRING, help="counter, airsignal.", required=True
)
@click.option("--platform", type=click.STRING, help="Platform (os).", required=True)
@click.option("--verbose", type=click.BOOL, help="Verbosity.", default=True)
@click.option(
    "--id", type=click.INT, help="Run ID to pair inlet and outlet.", required=True
)
def main(
    fs: int, mp: bool, ws: int, datatype: str, platform: str, verbose: bool, id: int
):
    logger = logger_creator(verbose)

    file_name = Path(
        f"./logs/id-{id}_inlet-archive8_datatype-{datatype}_platform-{platform}_multiproc-{str(mp)}_fs-{fs}_window-{ws}.csv"
    )
    click.echo(f"Logs: {file_name}")

    settings = SystemSettings(
        window_size=ws,
        fs=fs,
        multiproc=mp,
        log_file_name=file_name,
        stream_name=datatype,
        logger=logger,
    )
    system = System(settings)
    ez.run({"system": system})


if __name__ == "__main__":
    main()
