import logging
from typing import Any
from collections import deque
from collections.abc import AsyncGenerator

import pylsl
import ezmsg.core as ez

from lsl_comp.ez_utils.message import Message


class LSLOutletSettings(ez.Settings):
    fs: int
    stream_name: str


class LSLOutletState(ez.State):
    outlet: Any


class LSLOutletUnit(ez.Unit):
    STATE = LSLOutletState
    SETTINGS = LSLOutletSettings

    INPUT = ez.InputStream(Any)

    def initialize(self) -> None:
        info = pylsl.StreamInfo(
            name=self.SETTINGS.stream_name,
            type=self.SETTINGS.stream_name,
            channel_count=1,
            nominal_srate=self.SETTINGS.fs,
        )

        self.STATE.outlet = pylsl.StreamOutlet(info=info, chunk_size=0)

    @ez.subscriber(INPUT)
    async def outlet(self, message: Message) -> None:
        sample, timestamp = message.sample, message.timestamp
        self.STATE.outlet.push_sample([sample], timestamp)

        if sample == -1:
            raise ez.Complete


# ==================================================================
class LSLInletSettings(ez.Settings):
    fs: int
    window_size: int
    stream_name: str
    logger: logging.Logger


class LSLInletState(ez.State):
    inlet: Any
    buffer: deque


class LSLInletUnit(ez.Unit):
    STATE = LSLInletState
    SETTINGS = LSLInletSettings

    OUTPUT = ez.OutputStream(str)

    def initialize(self) -> None:
        streams = pylsl.resolve_byprop("name", self.SETTINGS.stream_name)
        self.STATE.inlet = pylsl.StreamInlet(streams[0], max_buflen=1)
        self.STATE.buffer = deque(maxlen=self.SETTINGS.window_size)

    @ez.publisher(OUTPUT)
    async def inlet(self) -> AsyncGenerator:
        while True:
            sample, t_generation = self.STATE.inlet.pull_sample()

            if sample and t_generation:
                sample = sample[0]

                # -1 sent after the last sample to gracefully close stream
                if sample == -1:
                    # write last remaining buffer to disk
                    if len(self.STATE.buffer) > 0:
                        self.SETTINGS.logger.debug("log the last remaining buffer...")
                        self.SETTINGS.logger.debug(
                            (
                                t_generation,
                                len(self.STATE.buffer),
                                f"{self.STATE.buffer[0][-1]}...{self.STATE.buffer[-1][-1]}",
                            )
                        )

                        log_line = [
                            ";".join((str(e) for e in b))
                            for b in list(zip(*self.STATE.buffer))
                        ]
                        log_line = ",".join(log_line) + "\n"

                        yield (self.OUTPUT, log_line)

                    # send the last -1 to stop downstream units
                    yield (self.OUTPUT, str(sample))

                    self.STATE.inlet.close_stream()
                    raise ez.Complete

                else:
                    t_arrival = pylsl.local_clock()
                    t_offset = self.STATE.inlet.time_correction()

                    if self.SETTINGS.window_size == 1:
                        self.SETTINGS.logger.debug((t_generation, sample))
                        log_line = f"{t_generation},{t_offset},{t_arrival},{sample}\n"

                        yield (self.OUTPUT, log_line)

                    else:
                        self.STATE.buffer.append(
                            (t_generation, t_offset, t_arrival, sample)
                        )

                        if len(self.STATE.buffer) == self.SETTINGS.window_size:
                            log_line = [
                                ";".join((str(e) for e in b))
                                for b in list(zip(*self.STATE.buffer))
                            ]
                            log_line = ",".join(log_line) + "\n"

                            yield (self.OUTPUT, log_line)

                            self.SETTINGS.logger.debug(
                                (
                                    t_generation,
                                    len(self.STATE.buffer),
                                    f"{self.STATE.buffer[0][-1]}...{self.STATE.buffer[-1][-1]}",
                                )
                            )
                            self.STATE.buffer.clear()
