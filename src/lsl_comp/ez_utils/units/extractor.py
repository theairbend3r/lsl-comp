from collections.abc import AsyncGenerator

import ezmsg.core as ez
from ezmsg.util.messages.axisarray import AxisArray
import pylsl

from lsl_comp.ez_utils.message import Message


class NSPExtractorSettings(ez.Settings):
    tc: int


class NSPExtractorState(ez.State):
    current_count: int


class NSPExtractorUnit(ez.Unit):
    SETTINGS = NSPExtractorSettings
    STATE = NSPExtractorState

    INPUT = ez.InputStream(AxisArray)
    OUTPUT = ez.OutputStream(Message)

    def initialize(self) -> None:
        self.STATE.current_count = 0

    @ez.subscriber(INPUT)
    @ez.publisher(OUTPUT)
    async def extract(self, message: AxisArray) -> AsyncGenerator:
        # NSPSource() returns a numpy array of shape (time, channels)

        if self.STATE.current_count >= self.SETTINGS.tc:
            yield (
                self.OUTPUT,
                Message(samples=[-1.0], timestamps=[99.99]),
            )
            raise ez.Complete
        else:
            # extract values from 2nd channel (index = 1) for all timesteps
            samples: list[float] = message.data[:, 1].tolist()

            # how many samples were actually received in a single message
            num_samples = len(samples)

            # extract timestamps received from hardware
            # WARN: maybe we should pylsl.local_clock() as the first timestamp
            # instead of reading it from the machine so it can be related back to markers timestamps
            gain: float = message.axes["time"].gain
            first_timestamp: float = message.axes["time"].offset.item()
            timestamps = [(first_timestamp + i * gain) for i in range(num_samples)]

            yield (
                self.OUTPUT,
                Message(samples=samples, timestamps=timestamps),
            )

            self.STATE.current_count += num_samples


# =============================
class AxisArrayExtractorUnit(ez.Unit):
    INPUT = ez.InputStream(AxisArray)
    OUTPUT = ez.OutputStream(str)

    @ez.subscriber(INPUT)
    @ez.publisher(OUTPUT)
    async def extract(self, message: AxisArray) -> AsyncGenerator:
        if message.data == [-1.0]:
            yield (self.OUTPUT, "-1.0")

            raise ez.Complete
        else:
            t_inlet = pylsl.local_clock()
            t_lsloffset = (
                99.99  # ezmsg-lsl provides no way to get pylsl's time_correction()
            )
            # extract values from 2nd channel (index = 1) for all timesteps
            samples: list[float] = message.data[:, 1].tolist()

            # how many samples were actually received in a single message
            num_samples = len(samples)

            # extract timestamps received from axisarray
            gain: float = message.axes["time"].gain
            first_timestamp: float = message.axes["time"].offset.item()
            timestamps = [(first_timestamp + i * gain) for i in range(num_samples)]

            log_line = f"{';'.join([str(t) for t in timestamps])},{t_lsloffset},{t_inlet},{';'.join(str(s) for s in samples)}\n"

            yield (self.OUTPUT, log_line)
