from pathlib import Path

import numpy as np
import polars as pl


# ====== helper functions =================
def assign_xlet_filename(log_files: list[Path]) -> tuple[Path, Path]:
    outlet_log_filename, inlet_log_filename = None, None

    xlets = {f.name.split("_")[1].split("-")[0]: f for f in log_files}

    inlet_log_filename = xlets["inlet"]
    outlet_log_filename = xlets["outlet"]

    # if inlet_log_filename is None or outlet_log_filename is None:
    #     raise ValueError("Error finding a log file each for inlet and outlet.")

    return outlet_log_filename, inlet_log_filename


def extract_metainfo(log_files: list[Path]):
    ids = [f.name.split("_")[0].split("-")[-1] for f in log_files]

    xlets = {
        f.name.split("_")[1].split("-")[0]: f.name.split("_")[1].split("-")[-1]
        for f in log_files
    }

    datatype = [f.name.split("_")[2].split("-")[-1] for f in log_files]
    platform = [f.name.split("_")[3].split("-")[-1] for f in log_files]
    multiproc = [f.name.split("_")[4].split("-")[-1] for f in log_files]
    fs = [f.name.split("_")[5].split("-")[-1] for f in log_files]
    window_size = [f.name.split("_")[6].split("-")[-1].split(".")[0] for f in log_files]

    assert (
        len(set(ids)) == 1
        and len(set(datatype)) == 1
        and len(set(platform)) == 1
        and len(set(multiproc)) == 1
        and len(set(fs)) == 1
        and len(set(window_size)) == 1
    )

    meta_info_run = {
        "id": int(ids[0]),
        "inlet": xlets["inlet"],
        "outlet": xlets["outlet"],
        "datatype": datatype[0],
        "platform": platform[0],
        "multiproc": multiproc[0] == "True",
        "fs": int(fs[0]),
        "window_size": int(window_size[0]),
    }

    return meta_info_run


def verify_data_loss(
    df_outlet: pl.DataFrame, df_inlet: pl.DataFrame, window_size: int
) -> bool:
    # outlet is started first and the the inlet.
    # the inlet will miss a few samples in the beginning in the time it takes
    # to establish connection between the inlet and the outlet.

    # this function verifies if all the data in the inlet is present in the outlet
    # i.e. is the inlet a continuous subset of the outlet

    outlet_number_arr = df_outlet["x"].to_numpy()

    if window_size == 1:
        inlet_number_arr = df_inlet["x"].to_numpy()
    else:
        inlet_number_arr = df_inlet["x"].explode().to_numpy()

    first_inlet_number = inlet_number_arr[0]
    first_inlet_number_idx_in_outlet = np.where(
        outlet_number_arr == first_inlet_number
    )[0].item()

    outlet_number_subset = outlet_number_arr[first_inlet_number_idx_in_outlet:]

    return not all(inlet_number_arr == outlet_number_subset)


def get_window_duration(df_inlet: pl.DataFrame, window_size: int) -> float | None:
    if window_size > 1:
        window_latency = df_inlet.with_columns(
            (
                pl.col("t_gen_outlet").list.last() - pl.col("t_gen_outlet").list.first()
            ).alias("window_time")
        )["window_time"].to_numpy()

        return np.mean(window_latency).item()
    else:
        return None


def get_average_latency(df_inlet: pl.DataFrame) -> float:
    t_gen_outlet = df_inlet["t_gen_outlet"].explode().to_numpy()
    t_arr_inlet = df_inlet["t_arr_inlet"].explode().to_numpy()

    return np.mean(t_arr_inlet - t_gen_outlet).item()


# ===========================================


basepath_logfiles = Path("./logs/")
log_files = list(basepath_logfiles.glob("*.csv"))

# "id": None,
# "outlet": None,
# "inlet": None,
# "datatype": None,
# "platform": None,
# "multiproc": None,
# "fs": None,
# "window_size": None,
dict_for_df = []


# get all run_ids
run_ids = [int(f.name.split("_")[0].split("-")[-1]) for f in log_files]
run_ids = sorted(list(set(run_ids)))

for run_id in run_ids:
    logfiles = [
        f for f in log_files if f.name.split("_")[0].split("-")[-1] == str(run_id)
    ]

    if len(logfiles) != 2:
        raise ValueError(
            f"More or less than the required 2 files; one for each outlet and inlet.\n{logfiles}"
        )

    meta_info = extract_metainfo(log_files=logfiles)
    outlet_log_filename, inlet_log_filename = assign_xlet_filename(log_files=logfiles)

    df_outlet = pl.read_csv(outlet_log_filename).with_columns(
        pl.col("x").cast(pl.Float64),
    )

    if meta_info["window_size"] == 1:
        df_inlet = pl.read_csv(inlet_log_filename)
    else:
        df_inlet = pl.read_csv(inlet_log_filename).with_columns(
            pl.col("x").str.split(";").cast(pl.List(pl.Float64)),
            pl.col("t_gen_outlet").str.split(";").cast(pl.List(pl.Float64)),
            pl.col("t_lsl_offset").str.split(";").cast(pl.List(pl.Float64)),
            pl.col("t_arr_inlet").str.split(";").cast(pl.List(pl.Float64)),
        )

    avg_latency = get_average_latency(df_inlet=df_inlet)
    is_data_loss = verify_data_loss(
        df_outlet=df_outlet, df_inlet=df_inlet, window_size=meta_info["window_size"]
    )
    avg_window_duration = get_window_duration(
        df_inlet=df_inlet, window_size=meta_info["window_size"]
    )

    dict_for_df.append(
        {
            **meta_info,
            **{
                "is_data_loss": is_data_loss,
                "avg_window_duration": avg_window_duration,
                "avg_latency": avg_latency,
            },
        }
    )


final_df = pl.from_dicts(dict_for_df)
pl.Config.set_tbl_rows(999)
pl.Config.set_tbl_cols(999)
print(final_df)
