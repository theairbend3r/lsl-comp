from pathlib import Path
from collections import deque

import pylsl
import click

from lsl_comp.utils.pylogger import logger_creator


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
    if datatype not in ["counter", "airsignal"]:
        raise ValueError("Incompatible datatype.")

    logger = logger_creator(verbose)

    file_name = Path(
        f"./logs/id-{id}_inlet-pylsl_datatype-{datatype}_platform-{platform}_multiproc-{str(mp)}_fs-{fs}_window-{ws}.csv"
    )
    click.echo(f"Logs: {file_name}")

    # init lsl stream
    streams = pylsl.resolve_byprop("name", datatype)
    inlet = pylsl.StreamInlet(streams[0], max_buflen=1)

    # set window size
    window_size = ws

    # init buffer for windowing
    if window_size != 1:
        buffer = deque(maxlen=window_size)

    # create log files
    file = open(file_name, "w")
    file.write(
        ",".join(
            [
                "t_gen_outlet",
                "t_lsl_offset",
                "t_arr_inlet",
                "x\n",
            ]
        )
    )

    while True:
        sample, t_gen_outlet = inlet.pull_sample()

        if sample and t_gen_outlet:
            sample = int(sample[0])

            # -1 sent after the last sample to gracefully close stream
            if sample == -1:
                break

            t_offset, t_arrival = inlet.time_correction(), pylsl.local_clock()

            if window_size == 1:
                logger.debug(("[pylsl-inlet] ", t_gen_outlet, sample))
                file.write(f"{t_gen_outlet},{t_offset},{t_arrival},{sample}\n")
            else:
                buffer.append((t_gen_outlet, t_offset, t_arrival, sample))
                if len(buffer) == window_size:
                    log_line = [
                        ";".join((str(e) for e in b)) for b in list(zip(*buffer))
                    ]
                    log_line = ",".join(log_line) + "\n"

                    file.write(log_line)

                    logger.debug(("[pylsl-inlet] ", t_gen_outlet, len(buffer)))

                    buffer.clear()

    # write last remaining buffer to disk
    # if last buffer is less than the window_size, then it is never written to disk
    if window_size > 1 and len(buffer) > 0:
        logger.debug("log the last remaining buffer...")
        log_line = [";".join((str(e) for e in b)) for b in list(zip(*buffer))]
        log_line = ",".join(log_line) + "\n"

        logger.debug(("[pylsl-inlet]", t_gen_outlet, len(buffer)))
        file.write(log_line)

    logger.info("closing inlet and writing logs to disk...")
    inlet.close_stream()
    file.flush()
    file.close()


if __name__ == "__main__":
    main()
