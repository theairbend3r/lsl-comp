import os
import sys
import asyncio
from typing import Any
from pathlib import Path
from dataclasses import dataclass
from collections.abc import AsyncGenerator

import pylsl
import click
import ezmsg.core as ez


@dataclass
class Message:
    sample: int | float
    timestamp: float


# ==================================================================
class CountSettings(ez.Settings):
    total_count: int
    fs: int


class CountUnit(ez.Unit):
    SETTINGS = CountSettings

    OUTPUT = ez.OutputStream(Message)

    @ez.publisher(OUTPUT)
    async def count(self) -> AsyncGenerator:
        start_time = pylsl.local_clock()
        sent_samples = 0
        n = 0

        while n < self.SETTINGS.total_count:
            elapsed_time = pylsl.local_clock() - start_time
            required_samples = int(self.SETTINGS.fs * elapsed_time) - sent_samples

            for _ in range(required_samples):
                yield (
                    self.OUTPUT,
                    Message(sample=n, timestamp=pylsl.local_clock()),
                )
                n += 1

            sent_samples += required_samples

            await asyncio.sleep(1 / self.SETTINGS.fs)

        yield (
            self.OUTPUT,
            Message(sample=-1, timestamp=pylsl.local_clock()),
        )

        raise ez.Complete


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


class CountSystemSettings(ez.Settings):
    total_count: int
    fs: int
    multiproc: bool
    log_file_name: Path
    stream_name: str


class CountSystem(ez.Collection):
    SETTINGS = CountSystemSettings

    COUNT = CountUnit()
    OUTLET = LSLOutletUnit()
    LOG = LogUnit()

    def configure(self) -> None:
        self.COUNT.apply_settings(
            CountSettings(total_count=self.SETTINGS.total_count, fs=self.SETTINGS.fs)
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
            (self.COUNT.OUTPUT, self.OUTLET.INPUT),
            (self.COUNT.OUTPUT, self.LOG.INPUT),
        )

    def process_components(self) -> tuple[ez.Component, ...]:
        if self.SETTINGS.multiproc:
            return (self.COUNT, self.OUTLET, self.LOG)
        else:
            return ()


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

    settings = CountSystemSettings(
        total_count=tc,
        fs=fs,
        multiproc=mp,
        log_file_name=file_name,
        stream_name=datatype,
    )
    system = CountSystem(settings)
    ez.run({"system": system})


if __name__ == "__main__":
    main()
