import os
import sys
import time
import pylsl
import click
from pathlib import Path


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
        sys.stdout = open(os.devnull, "w")

    file_name = Path(
        f"./logs/id-{id}_outlet-pylsl_datatype-{datatype}_platform-{platform}_multiproc-{str(mp)}_fs-{fs}_window-{ws}.csv"
    )
    click.echo(f"Logs: {file_name}")

    # create log files
    file = open(file_name, "w")
    file.write(
        ",".join(
            [
                "t_gen_outlet",
                "x\n",
            ]
        )
    )

    # create lsl stream
    info = pylsl.StreamInfo(
        name=datatype, type=datatype, channel_count=1, nominal_srate=fs
    )
    outlet = pylsl.StreamOutlet(info=info, chunk_size=0, max_buffered=360)

    start_time = pylsl.local_clock()
    sent_samples = 0
    total_count = tc
    n = 0

    while n < total_count:
        elapsed_time = pylsl.local_clock() - start_time
        required_samples = int(fs * elapsed_time) - sent_samples

        for sample_ix in range(required_samples):
            mysample = [n]
            curr_time = pylsl.local_clock()
            outlet.push_sample(mysample, curr_time)

            file.write(f"{curr_time},{n}\n")
            print(curr_time - start_time, curr_time, mysample)
            n += 1

        sent_samples += required_samples
        time.sleep(1 / fs)

    print("closing outlet and writing logs to disk...")
    outlet.push_sample([-1], pylsl.local_clock())
    file.flush()
    file.close()


if __name__ == "__main__":
    main()
