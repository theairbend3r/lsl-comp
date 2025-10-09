import os
import sys
from typing import Any
from pathlib import Path
from collections import deque
from collections.abc import AsyncGenerator

import click
import pylsl
import ezmsg.core as ez


# ==================================================================
class LSLInletSettings(ez.Settings):
    fs: int
    window_size: int
    stream_name: str


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

    @ez.publisher(OUTPUT)
    async def inlet(self) -> AsyncGenerator:
        while True:
            sample, t_generation = self.STATE.inlet.pull_sample()

            if sample and t_generation:
                sample = sample[0]

                # -1 sent after the last sample to gracefully close stream
                if sample == -1:
                    # write last remaining buffer to disk
                    if len(self.STATE.buffer) > 0:
                        print("log the last remaining buffer...")
                        print(
                            t_generation,
                            len(self.STATE.buffer),
                            f"{self.STATE.buffer[0][-1]}...{self.STATE.buffer[-1][-1]}",
                        )

                        log_line = [
                            ";".join((str(e) for e in b))
                            for b in list(zip(*self.STATE.buffer))
                        ]
                        log_line = ",".join(log_line) + "\n"

                        yield (self.OUTPUT, log_line)

                    # send the last -1 to stop downstream units
                    yield (self.OUTPUT, str(sample))

                    self.STATE.inlet.close_stream()
                    raise ez.Complete

                else:
                    t_arrival = pylsl.local_clock()
                    t_offset = self.STATE.inlet.time_correction()

                    if self.SETTINGS.window_size == 1:
                        print(t_generation, sample)
                        log_line = f"{t_generation},{t_offset},{t_arrival},{sample}\n"

                        yield (self.OUTPUT, log_line)

                    else:
                        self.STATE.buffer.append(
                            (t_generation, t_offset, t_arrival, sample)
                        )

                        if len(self.STATE.buffer) == self.SETTINGS.window_size:
                            log_line = [
                                ";".join((str(e) for e in b))
                                for b in list(zip(*self.STATE.buffer))
                            ]
                            log_line = ",".join(log_line) + "\n"

                            yield (self.OUTPUT, log_line)

                            print(
                                t_generation,
                                len(self.STATE.buffer),
                                f"{self.STATE.buffer[0][-1]}...{self.STATE.buffer[-1][-1]}",
                            )
                            self.STATE.buffer.clear()


# ==================================================================
class LogSettings(ez.Settings):
    window_size: int
    log_file_name: Path


class LogState(ez.State):
    file: Any


class LogUnit(ez.Unit):
    STATE = LogState
    SETTINGS = LogSettings
    INPUT = ez.InputStream(str)

    def initialize(self) -> None:
        self.STATE.file = open(self.SETTINGS.log_file_name, "w")

        self.STATE.file.write(
            ",".join(
                [
                    "t_gen_outlet",
                    "t_lsl_offset",
                    "t_arr_inlet",
                    "x\n",
                ]
            )
        )

    @ez.subscriber(INPUT)
    async def on_message(self, message: str) -> None:
        if message == "-1.0":
            print("closing inlet and writing logs to disk...")
            self.STATE.file.flush()
            self.STATE.file.close()

            raise ez.Complete
        else:
            self.STATE.file.write(message)


# ==================================================================
class CountSystemSettings(ez.Settings):
    fs: int
    window_size: int
    multiproc: bool
    log_file_name: Path
    stream_name: str


class CountSystem(ez.Collection):
    SETTINGS = CountSystemSettings

    INLET = LSLInletUnit()
    LOG = LogUnit()

    def configure(self) -> None:
        self.INLET.apply_settings(
            (
                LSLInletSettings(
                    fs=self.SETTINGS.fs,
                    window_size=self.SETTINGS.window_size,
                    stream_name=self.SETTINGS.stream_name,
                )
            )
        )

        self.LOG.apply_settings(
            LogSettings(
                log_file_name=self.SETTINGS.log_file_name,
                window_size=self.SETTINGS.window_size,
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
    if not verbose:
        sys.stdout = open(os.devnull, "w")

    file_name = Path(
        f"./logs/id-{id}_inlet-ezmsgpylsl_datatype-{datatype}_platform-{platform}_multiproc-{str(mp)}_fs-{fs}_window-{ws}.csv"
    )
    click.echo(f"Logs: {file_name}")

    settings = CountSystemSettings(
        window_size=ws,
        fs=fs,
        multiproc=mp,
        log_file_name=file_name,
        stream_name=datatype,
    )
    system = CountSystem(settings)
    ez.run({"system": system})


if __name__ == "__main__":
    main()
