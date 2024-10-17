import pydantic
import pytest

from papa_events.exceptions import PapaException


class Event(pydantic.BaseModel):
    name: str
    age: int


@pytest.mark.asyncio
async def test_init_new_event_error(app):
    with pytest.raises(PapaException, match=r"Init needed"):
        await app.new_event("test_event.new", {"payload": "payload"})


@pytest.mark.asyncio
async def test_init_stop(app):
    with pytest.raises(PapaException, match=r"Init needed"):
        await app.stop()
