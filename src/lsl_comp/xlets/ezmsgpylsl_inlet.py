import logging
from pathlib import Path

import click
import ezmsg.core as ez

from lsl_comp.utils.pylogger import logger_creator
from lsl_comp.ez_utils.units.log import LogInletSettings, LogInletUnit
from lsl_comp.ez_utils.units.lsl import LSLInletSettings, LSLInletUnit


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
        f"./logs/id-{id}_inlet-ezlsl_datatype-{datatype}_platform-{platform}_multiproc-{str(mp)}_fs-{fs}_window-{ws}.csv"
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
