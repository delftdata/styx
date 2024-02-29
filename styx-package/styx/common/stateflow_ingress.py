from dataclasses import dataclass
from enum import Enum, auto


@dataclass
class StateflowIngress(object):
    host: str
    port: int
    ext_host: str
    ext_port: int


class IngressTypes(Enum):
    KAFKA = auto()
