from collections.abc import Callable
from dataclasses import dataclass

import pydantic


@dataclass
class CallBack:
    function: Callable
    param_name: str
    param_model: pydantic.BaseModel


@dataclass
class UseCase:
    name: str
    callback: CallBack
    event_names: list[str]
    retries: int
