import time
import platform
import itertools
import subprocess
import multiprocessing
from pathlib import Path
from typing import NamedTuple

import click

from lsl_comp.utils.pylogger import logger_creator

logger = logger_creator(verbose=True)

outlet_to_script = {
    "ezmsg_pylsl": Path("./src/lsl_comp/xlets/ezmsgpylsl_outlet.py"),
    "pylsl": Path("./src/lsl_comp/xlets/pylsl_outlet.py"),
}
inlet_to_script = {
    "ezmsg_pylsl": Path("./src/lsl_comp/xlets/ezmsgpylsl_inlet.py"),
    "pylsl": Path("./src/lsl_comp/xlets/pylsl_inlet.py"),
}


def run_script(script_name: str, args: list[str]):
    try:
        if platform.system() == "Windows":
            _ = subprocess.run(
                ["python", script_name] + args,
                check=True,
                executable=r".venv\Scripts\python.exe",
            )
        else:
            _ = subprocess.run(["python", script_name] + args, check=True)
    except subprocess.CalledProcessError as e:
        logger.error(f"Error occurred while running {script_name}: {e}")


class Combo(NamedTuple):
    platform: str
    datatype: str
    outlet: str
    inlet: str
    total_count: int
    fs: int
    multiproc: bool
    window_size: int


@click.command()
@click.option(
    "--platform", type=click.STRING, help="OS (windows/debian/macos).", required=True
)
@click.option(
    "--datatype", type=click.STRING, help="counter, airsignal.", required=True
)
def main(platform: str, datatype: str) -> None:
    # different configurations
    outlets = ["ezmsg_pylsl", "pylsl"]
    inlets = ["ezmsg_pylsl", "pylsl"]
    total_count = [3_000]
    sampling_rate = [1000]
    multiproc = [True, False]
    window_size = [1, 60, 100]

    # create combos from above list
    combos = list(
        itertools.product(
            [platform],
            [datatype],
            outlets,
            inlets,
            total_count,
            sampling_rate,
            multiproc,
            window_size,
        )
    )

    logger.info(f"\nTotal combos = {len(combos)}\n")
    combos = [Combo(*c) for c in combos]

    # ignore multiprocessing where either the inlet or outlet is pure pylsl
    combos = [
        c
        for c in combos
        if not ((c.outlet == "pylsl" or c.inlet == "pylsl") and c.multiproc)
    ]
    logger.info(f"\nValid combos = {len(combos)}\n")

    for i, c in enumerate(combos):
        logger.debug("=" * 50)
        logger.debug(("\n", i, c, "\n"))

        log_file_outlet = outlet_to_script[c.outlet]
        log_file_inlet = inlet_to_script[c.inlet]
        dt = c.datatype
        tc = c.total_count
        fs = c.fs
        mp = c.multiproc
        ws = c.window_size

        logger.debug((c.outlet, log_file_outlet))
        logger.debug((c.inlet, log_file_inlet))

        process_outlet = multiprocessing.Process(
            target=run_script,
            args=(
                log_file_outlet,
                f"--tc {tc} --fs {fs} --mp {mp} --ws {ws} --datatype {dt} --platform {platform} --verbose False --id {i}".split(
                    " "
                ),
            ),
        )
        process_inlet = multiprocessing.Process(
            target=run_script,
            args=(
                log_file_inlet,
                f"--fs {fs} --mp {mp} --ws {ws} --datatype {dt} --platform {platform} --verbose False --id {i}".split(
                    " "
                ),
            ),
        )

        process_outlet.start()
        process_inlet.start()

        process_outlet.join()
        process_inlet.join()

        time.sleep(1)


if __name__ == "__main__":
    main()
