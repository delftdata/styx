from dataclasses import dataclass


@dataclass
class StateflowWorker(object):
    host: str
    port: int
    protocol_port: int
