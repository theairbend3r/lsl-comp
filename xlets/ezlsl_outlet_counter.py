import asyncio
from typing import Any
from collections.abc import AsyncGenerator

import numpy as np

import pylsl
import ezmsg.core as ez
from ezmsg.util.messages.axisarray import AxisArray
from ezmsg.lsl.outlet import LSLOutletSettings, LSLOutletUnit


# ==================================================================


class CountSettings(ez.Settings):
    total_count: int
    fs: int


class CountUnit(ez.Unit):
    SETTINGS = CountSettings

    OUTPUT = ez.OutputStream(AxisArray)
    OUTPUT_LOG = ez.OutputStream(AxisArray)

    @ez.publisher(OUTPUT)
    async def count(self) -> AsyncGenerator:
        start_time = pylsl.local_clock()
        sent_samples = 0
        n = 0

        while n < self.SETTINGS.total_count:
            elapsed_time = pylsl.local_clock() - start_time
            required_samples = int(self.SETTINGS.fs * elapsed_time) - sent_samples

            for _ in range(required_samples):
                mysample = AxisArray(
                    data=np.array([n])[:, np.newaxis],
                    dims=["time", "ch"],
                    axes={
                        "time": AxisArray.TimeAxis(fs=self.SETTINGS.fs),
                        "ch": AxisArray.CoordinateAxis(
                            data=np.array(["Ch0"]),
                            dims=["ch"],
                        ),
                    },
                )
                yield (self.OUTPUT, mysample)
                n += 1

            sent_samples += required_samples

            await asyncio.sleep(1 / self.SETTINGS.fs)

        yield (
            self.OUTPUT,
            AxisArray(
                data=np.array([-1])[:, np.newaxis],
                dims=["time", "ch"],
                axes={
                    "time": AxisArray.TimeAxis(fs=self.SETTINGS.fs),
                    "ch": AxisArray.CoordinateAxis(
                        data=np.array([f"Ch{_}" for _ in range(1)]),
                        dims=["ch"],
                    ),
                },
            ),
        )

        raise ez.Complete


# ==================================================================


class LogState(ez.State):
    file: Any


class LogUnit(ez.Unit):
    STATE = LogState
    INPUT = ez.InputStream(Any)

    def initialize(self) -> None:
        self.STATE.file = open("./logs/lslcomp/ezlsl-outlet-counter-push.csv", "w")
        self.STATE.file.write(
            ",".join(
                [
                    "t_dep_outlet",
                    "x\n",
                ]
            )
        )

    @ez.subscriber(INPUT)
    async def on_message(self, message: AxisArray) -> None:
        message = message.data.item()

        curr_time = pylsl.local_clock()
        print(curr_time, message)

        if message == -1:
            print("closing outlet and writing logs to disk...")
            self.STATE.file.flush()
            self.STATE.file.close()

            raise ez.Complete

        else:
            self.STATE.file.write(f"{curr_time},{message}\n")


# ==================================================================
class CountSystemSettings(ez.Settings):
    total_count: int
    fs: int


class CountSystem(ez.Collection):
    SETTINGS = CountSystemSettings

    COUNT = CountUnit()
    LOG = LogUnit()
    LSL_OUTLET = LSLOutletUnit()

    def configure(self) -> None:
        self.COUNT.apply_settings(
            CountSettings(total_count=self.SETTINGS.total_count, fs=self.SETTINGS.fs)
        )
        self.LSL_OUTLET.apply_settings(
            LSLOutletSettings(stream_name="counter", stream_type="counter")
        )

    def network(self) -> ez.NetworkDefinition:
        return (
            (self.COUNT.OUTPUT, self.LSL_OUTLET.INPUT_SIGNAL),
            (self.COUNT.OUTPUT, self.LOG.INPUT),
            # (self.COUNT.OUTPUT_LOG, self.LOG_COUNT.INPUT_MESSAGE),
        )

    # def process_components(self) -> tuple[ez.Component, ...]:
    #     return (self.COUNT, self.PRINT, self.LOG_COUNT, self.LSL_OUTLET)


if __name__ == "__main__":
    settings = CountSystemSettings(total_count=2000, fs=1000)
    system = CountSystem(settings)
    ez.run({"system": system})
