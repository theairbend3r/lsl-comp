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
    logger: logging.Logger


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
        samples, timestamps = message.samples, message.timestamps
        self.STATE.outlet.push_chunk(samples, timestamps)

        if samples == [-1.0]:
            self.SETTINGS.logger.info("Closing outlet.")
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

    def buffer_print(self, buffer, logger) -> None:
        logger.debug((len(buffer), f"{buffer[0][-1]}...{buffer[-1][-1]}"))

    def buffer_to_string(self, buffer) -> str:
        log_line = [";".join((str(e) for e in b)) for b in list(zip(*buffer))]
        log_line = ",".join(log_line) + "\n"

        return log_line

    @ez.publisher(OUTPUT)
    async def inlet(self) -> AsyncGenerator:
        while True:
            # samples is a list[list[float]]
            # t_outlets is a list[float]
            pulled_sample: tuple[list[list[float]], list[float]] = (
                self.STATE.inlet.pull_chunk()
            )
            samples, t_outlets = pulled_sample

            if samples and t_outlets:
                samples = [s[0] for s in samples]
                # [-1.0] sent after the last sample to gracefully close stream
                if samples == [-1.0]:
                    if len(self.STATE.buffer) > 0:
                        self.SETTINGS.logger.info("Log last remaining buffer.")
                        self.buffer_print(self.STATE.buffer, self.SETTINGS.logger)
                        log_line = self.buffer_to_string(self.STATE.buffer)
                        yield (self.OUTPUT, log_line)

                    self.SETTINGS.logger.debug((t_outlets, str(samples[0])))
                    yield (self.OUTPUT, str(samples[0]))

                    self.SETTINGS.logger.info("Closing inlet.")
                    self.STATE.inlet.close_stream()

                    raise ez.Complete
                else:
                    t_inlet = pylsl.local_clock()
                    t_lsloffset = self.STATE.inlet.time_correction()

                    # match self.SETTINGS.window_size:
                    if self.SETTINGS.window_size == 0:
                        self.SETTINGS.logger.debug(
                            (len(samples), f"{samples[0]}...{samples[-1]}")
                        )
                        log_line = f"{';'.join([str(t) for t in t_outlets])},{t_lsloffset},{t_inlet},{';'.join(str(s) for s in samples)}\n"

                        yield (self.OUTPUT, log_line)
                    elif self.SETTINGS.window_size > 0:
                        samples_info = [
                            (to, t_lsloffset, t_inlet, s)
                            for to, s in zip(t_outlets, samples)
                        ]

                        self.STATE.buffer.extend(samples_info)

                        if len(self.STATE.buffer) == self.SETTINGS.window_size:
                            self.buffer_print(self.STATE.buffer, self.SETTINGS.logger)
                            log_line = self.buffer_to_string(self.STATE.buffer)

                            yield (self.OUTPUT, log_line)

                            self.STATE.buffer.clear()
                    else:
                        raise ValueError("Incorrect window size.")
