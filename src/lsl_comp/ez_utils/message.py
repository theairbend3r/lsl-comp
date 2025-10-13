from dataclasses import dataclass


@dataclass
class Message:
    sample: int | float
    timestamp: float
