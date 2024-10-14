from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

import pydantic
from pydantic import model_validator
from pydantic_core import to_json


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


class OutputEvent(pydantic.BaseModel):
    name: str
    payload: bytes

    @model_validator(mode="before")
    @classmethod
    def valid_json(cls, data: Any) -> str:
        if isinstance(data, dict):
            if isinstance(data["payload"], pydantic.BaseModel):
                data["payload"] = data["payload"].model_dump_json(exclude_none=True).encode()
            elif isinstance(data["payload"], dict):
                try:
                    data["payload"] = to_json(data["payload"])
                except pydantic.ValidationError as exc:
                    raise ValueError("payload is not JSONable") from exc
        return data
