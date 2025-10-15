from dataclasses import dataclass


@dataclass
class Message:
    samples: list[float]
    timestamps: list[float]
