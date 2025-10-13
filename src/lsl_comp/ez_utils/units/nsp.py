from collections.abc import AsyncGenerator

import pylsl
import ezmsg.core as ez
from ezmsg.util.messages.axisarray import AxisArray

from lsl_comp.ez_utils.message import Message


class NSPExtractorSettings(ez.Settings):
    tc: int


class NSPExtractorState(ez.State):
    current_count: int


class NSPExtractorUnit(ez.Unit):
    SETTINGS = NSPExtractorSettings
    STATE = NSPExtractorState

    INPUT = ez.InputStream(AxisArray)
    OUTPUT = ez.InputStream(Message)

    async def extract(self, message: AxisArray) -> AsyncGenerator:
        if self.STATE.current_count >= self.SETTINGS.tc:
            yield (
                self.OUTPUT,
                Message(sample=-1, timestamp=pylsl.local_clock()),
            )
            raise ez.Complete
        else:
            sample = message.data[:, 1].item()
            yield (
                self.OUTPUT,
                Message(sample=sample, timestamp=pylsl.local_clock()),
            )
            self.STATE.current_count += 1
