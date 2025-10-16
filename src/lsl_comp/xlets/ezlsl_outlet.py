import sys
import logging
from pathlib import Path

import click
import ezmsg.core as ez
from ezmsg.lsl.units import LSLOutletUnit, LSLOutletSettings
from ezmsg.blackrock.nsp import NSPSource, NSPSourceSettings


from lsl_comp.utils.pylogger import logger_creator
from lsl_comp.ez_utils.units.log import LogOutletSettings, LogOutletUnit
from lsl_comp.ez_utils.units.extractor import NSPExtractorSettings, NSPExtractorUnit


# ==================================================================


class SystemSettings(ez.Settings):
    total_count: int
    fs: int
    multiproc: bool
    log_file_name: Path
    stream_name: str
    logger: logging.Logger


# ==================================================================


# class CountSystem(ez.Collection):
#     SETTINGS = SystemSettings
#
#     COUNT = CountUnit()
#     OUTLET = LSLOutletUnit()
#     LOG = LogOutletUnit()
#
#     def configure(self) -> None:
#         self.COUNT.apply_settings(
#             CountSettings(total_count=self.SETTINGS.total_count, fs=self.SETTINGS.fs)
#         )
#
#         self.OUTLET.apply_settings(
#             (
#                 LSLOutletSettings(
#                     fs=self.SETTINGS.fs,
#                     stream_name=self.SETTINGS.stream_name,
#                     logger=self.SETTINGS.logger,
#                 )
#             )
#         )
#         self.LOG.apply_settings(
#             LogOutletSettings(
#                 log_file_name=self.SETTINGS.log_file_name, logger=self.SETTINGS.logger
#             )
#         )
#
#     def network(self) -> ez.NetworkDefinition:
#         return (
#             (self.COUNT.OUTPUT, self.OUTLET.INPUT),
#             (self.COUNT.OUTPUT, self.LOG.INPUT),
#         )
#
#     def process_components(self) -> tuple[ez.Component, ...]:
#         if self.SETTINGS.multiproc:
#             return (self.COUNT, self.OUTLET, self.LOG)
#         else:
#             return ()
#

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
        f"./logs/id-{id}_outlet-ezmsgpylsl_datatype-{datatype}_platform-{platform}_multiproc-{str(mp)}_fs-{fs}_window-{ws}.csv"
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

    if datatype == "counter":
        # system = CountSystem(settings)
        pass
    elif datatype == "airsignal":
        system = AirsignalSystem(settings)
    else:
        raise ValueError("Incompatible datatype.")

    ez.run({"system": system})


if __name__ == "__main__":
    main()
