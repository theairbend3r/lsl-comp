from typing import Any
from collections.abc import AsyncGenerator


import pylsl
import ezmsg.core as ez
from ezmsg.sigproc.window import Window, WindowSettings
from ezmsg.lsl.inlet import AxisArray, LSLInfo, LSLInletSettings, LSLInletUnit


# ==================================================================
class ExtractUnit(ez.Unit):
    INPUT = ez.InputStream(AxisArray)
    OUTPUT = ez.OutputStream(str)

    @ez.subscriber(INPUT)
    @ez.publisher(OUTPUT)
    async def on_message(self, message: AxisArray):
        print(message.data[:, 0])
        # yield (self.OUTPUT, message.data[:, 0])
        # yield (self.OUTPUT, message.data[:, 0].item())


# ==================================================================


class LogSettings(ez.Settings):
    window_size: int


class LogState(ez.State):
    file: Any


class LogUnit(ez.Unit):
    STATE = LogState
    SETTINGS = LogSettings
    INPUT = ez.InputStream(Any)

    def initialize(self) -> None:
        if self.SETTINGS.window_size == 1:
            pullmethod = "pullsample"
        else:
            pullmethod = f"pullsample-window{self.SETTINGS.window_size}"

        self.STATE.file = open(
            f"./logs/lslcomp/ezmsg-pylsl-inlet-counter-{pullmethod}.csv", "w"
        )
        self.STATE.file.write(
            ",".join(
                [
                    "t_dep_outlet",
                    "t_lsl_offset",
                    "t_arr_inlet",
                    "x\n",
                ]
            )
        )

    @ez.subscriber(INPUT)
    async def on_message(self, message: str) -> None:
        if message == "-1":
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


class CountSystem(ez.Collection):
    SETTINGS = CountSystemSettings

    LSL_INLET = LSLInletUnit()
    WINDOW = Window()
    EXTRACT = ExtractUnit()
    LOG = LogUnit()

    def configure(self) -> None:
        self.LSL_INLET.apply_settings(
            LSLInletSettings(
                LSLInfo(
                    name="counter", type="counter", channel_count=1, nominal_srate=1000
                ),
            )
        )
        self.WINDOW.apply_settings(
            WindowSettings(
                axis="time",
                window_dur=1,
                window_shift=1,
                zero_pad_until="full",
            )
        )
        self.LOG.apply_settings(LogSettings(window_size=self.SETTINGS.window_size))

    def network(self) -> ez.NetworkDefinition:
        return (
            # without windowing
            (self.LSL_INLET.OUTPUT_SIGNAL, self.EXTRACT.INPUT),
        )

    # def process_components(self) -> tuple[ez.Component, ...]:
    #     return (self.LSL_INLET, self.PRINT, self.LOG_COUNT, self.WINDOW)


if __name__ == "__main__":
    settings = CountSystemSettings(window_size=1, fs=1000)
    system = CountSystem(settings)
    ez.run({"system": system})
