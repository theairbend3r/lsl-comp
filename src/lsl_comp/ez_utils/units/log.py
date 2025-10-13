from typing import Any
from pathlib import Path

import ezmsg.core as ez

from lsl_comp.ez_utils.message import Message


class LogOutletSettings(ez.Settings):
    log_file_name: Path


class LogOutletState(ez.State):
    file: Any


class LogOutletUnit(ez.Unit):
    SETTINGS = LogOutletSettings
    STATE = LogOutletState
    INPUT = ez.InputStream(Any)

    def initialize(self) -> None:
        self.STATE.file = open(self.SETTINGS.log_file_name, "w")
        self.STATE.file.write(
            ",".join(
                [
                    "t_gen_outlet",
                    "x\n",
                ]
            )
        )

    @ez.subscriber(INPUT)
    async def on_message(self, message: Message) -> None:
        sample, timestamp = message.sample, message.timestamp

        print(timestamp, sample)

        if sample == -1:
            print("closing outlet and writing logs to disk...")
            self.STATE.file.flush()
            self.STATE.file.close()

            raise ez.Complete

        else:
            self.STATE.file.write(f"{timestamp},{sample}\n")


# ==================================================================


class LogInletSettings(ez.Settings):
    window_size: int
    log_file_name: Path


class LogInletState(ez.State):
    file: Any


class LogInletUnit(ez.Unit):
    STATE = LogInletState
    SETTINGS = LogInletSettings
    INPUT = ez.InputStream(str)

    def initialize(self) -> None:
        self.STATE.file = open(self.SETTINGS.log_file_name, "w")

        self.STATE.file.write(
            ",".join(
                [
                    "t_gen_outlet",
                    "t_lsl_offset",
                    "t_arr_inlet",
                    "x\n",
                ]
            )
        )

    @ez.subscriber(INPUT)
    async def on_message(self, message: str) -> None:
        if message == "-1.0":
            print("closing inlet and writing logs to disk...")
            self.STATE.file.flush()
            self.STATE.file.close()

            raise ez.Complete
        else:
            self.STATE.file.write(message)
