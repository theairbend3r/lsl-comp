from collections import deque
from collections.abc import AsyncGenerator
import logging
from pathlib import Path
from typing import Any

import click
import ezmsg.core as ez
import pylsl

from lsl_comp.utils.pylogger import logger_creator
from lsl_comp.ez_utils.units.log import LogInletSettings, LogInletUnit


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
        self.STATE.inlet = pylsl.StreamInlet(streams[0], max_buflen=1)
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
            chunk, timestamp = self.STATE.inlet.pull_sample()
            if timestamp:
                print(chunk)
                print(chunk[0])
                print()
                # time_offset = self.STATE.inlet.time_correction()
                # timestamp_arrival_inlet = pylsl.local_clock()
                # self.STATE.buffer.append(
                #     (chunk[0], timestamp, time_offset, timestamp_arrival_inlet)
                # )
                #
                # if len(self.STATE.buffer) == self.SETTINGS.window_size:
                #     buffer_list = list(self.STATE.buffer)
                #     yield (self.OUTPUT, buffer_list)
                #     self.STATE.buffer.clear()
        # while True:
        #     samples, t_outlets = self.STATE.inlet.pull_sample()
        #
        #     if samples and t_outlets:
        #         print(samples, t_outlets)
        # samples = [s[0] for s in samples]
        # t_inlet = pylsl.local_clock()
        # t_lsloffset = self.STATE.inlet.time_correction()
        #
        # # match self.SETTINGS.window_size:
        # if self.SETTINGS.window_size == 0:
        #     self.SETTINGS.logger.debug(
        #         (len(samples), f"{samples[0]}...{samples[-1]}")
        #     )
        #     log_line = f"{';'.join([str(t) for t in t_outlets])},{t_lsloffset},{t_inlet},{';'.join(str(s) for s in samples)}\n"
        #
        #     yield (self.OUTPUT, log_line)
        # elif self.SETTINGS.window_size > 0:
        #     samples_info = [
        #         (to, t_lsloffset, t_inlet, s)
        #         for to, s in zip(t_outlets, samples)
        #     ]
        #
        #     self.STATE.buffer.extend(samples_info)
        #
        #     if len(self.STATE.buffer) == self.SETTINGS.window_size:
        #         self.buffer_print(self.STATE.buffer, self.SETTINGS.logger)
        #         log_line = self.buffer_to_string(self.STATE.buffer)
        #
        #         yield (self.OUTPUT, log_line)
        #
        #         self.STATE.buffer.clear()
        # else:
        #     raise ValueError("Incorrect window size.")


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
