import asyncio
from collections.abc import AsyncGenerator

import pylsl
import ezmsg.core as ez

from lsl_comp.ez_utils.message import Message


class CountSettings(ez.Settings):
    total_count: int
    fs: int


class CountUnit(ez.Unit):
    SETTINGS = CountSettings

    OUTPUT = ez.OutputStream(Message)

    @ez.publisher(OUTPUT)
    async def count(self) -> AsyncGenerator:
        start_time = pylsl.local_clock()
        sent_samples = 0
        n = 0

        while n < self.SETTINGS.total_count:
            elapsed_time = pylsl.local_clock() - start_time
            required_samples = int(self.SETTINGS.fs * elapsed_time) - sent_samples

            for _ in range(required_samples):
                yield (
                    self.OUTPUT,
                    Message(sample=n, timestamp=pylsl.local_clock()),
                )
                n += 1

            sent_samples += required_samples

            await asyncio.sleep(1 / self.SETTINGS.fs)

        yield (
            self.OUTPUT,
            Message(sample=-1, timestamp=pylsl.local_clock()),
        )

        raise ez.Complete
