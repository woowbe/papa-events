import pydantic
import pytest

from papa_events.exceptions import PapaException


class Event(pydantic.BaseModel):
    name: str
    age: int


@pytest.mark.asyncio
async def test_none_param(app):
    async def wrong_callback(): ...

    with pytest.raises(PapaException, match=r"You need one pydantic.BaseModel function param"):
        app.on_event(["test_event.new"], "test_event_use_case")(wrong_callback)


@pytest.mark.asyncio
async def test_wrong_event_param(app):
    async def wrong_callback(num: int): ...

    with pytest.raises(PapaException, match=r"You need one pydantic.BaseModel function param"):
        app.on_event(["test_event.new"], "test_event_use_case")(wrong_callback)


@pytest.mark.asyncio
async def test_missing_event_name_param(app):
    async def wrong_callback(event: Event): ...

    with pytest.raises(PapaException, match=r"You need one 'event_name' function param"):
        app.on_event(["test_event.new"], "test_event_use_case")(wrong_callback)

@pytest.mark.asyncio
async def test_duplicate_event(app):
    async def wrong_callback(event_name, event: Event): ...

    with pytest.raises(PapaException, match=r"^Duplicate functions for"):
        app.on_event(["test_event.new"], "test_event_use_case")(wrong_callback)
        app.on_event(["test_event.new"], "test_event_use_case")(wrong_callback)
