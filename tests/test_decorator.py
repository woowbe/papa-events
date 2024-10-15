import pydantic
import pytest

from papa_events.application import PapaApp
from papa_events.exceptions import PapaException


class Event(pydantic.BaseModel):
    name: str
    age: int


@pytest.mark.asyncio
async def test_none_param():
    async def wrong_callback(): ...

    app = PapaApp(broker_uri="")
    with pytest.raises(PapaException, match=r"You need one pydantic.BaseModel function param") as excinfo:
        app.on_event(["test_event.new"], "test_event_use_case")(wrong_callback)


@pytest.mark.asyncio
async def test_wrong_event_param():
    async def wrong_callback(num: int): ...

    app = PapaApp(broker_uri="")
    with pytest.raises(PapaException, match=r"You need one pydantic.BaseModel function param") as excinfo:
        app.on_event(["test_event.new"], "test_event_use_case")(wrong_callback)


@pytest.mark.asyncio
async def test_duplicate_event():
    async def wrong_callback(event: Event): ...

    app = PapaApp(broker_uri="")
    with pytest.raises(PapaException, match=r"^Duplicate functions for") as excinfo:
        app.on_event(["test_event.new"], "test_event_use_case")(wrong_callback)
        app.on_event(["test_event.new"], "test_event_use_case")(wrong_callback)
