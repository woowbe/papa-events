import asyncio
import json
from unittest.mock import create_autospec

import aio_pika
import pydantic
import pytest


class Event(pydantic.BaseModel):
    name: str
    age: int


@pytest.mark.asyncio
async def test_simple_event(app):
    async def welcome_email(event: Event): ...

    mocked_callback = create_autospec(welcome_email)

    app.on_event(["user.created"], "test_simple_event_use_case")(mocked_callback)
    await app.start()

    ev = Event(name="test name", age=25)
    await app.new_event("user.created", ev.model_dump_json().encode())
    await asyncio.sleep(2)

    await app.stop()

    mocked_callback.assert_awaited()
    mocked_callback.assert_called_once_with(event=ev)


@pytest.mark.asyncio
async def test_multiple_events(app):
    async def welcome_email(event: Event): ...

    async def register_analitics(event: Event): ...

    mocked_callback1 = create_autospec(welcome_email)
    mocked_callback2 = create_autospec(register_analitics)

    app.on_event(["user.created"], "test_multiple_events_use_case1")(mocked_callback1)
    app.on_event(["user.*"], "test_multiple_events_use_case2")(mocked_callback2)
    await app.start()

    ev = Event(name="test name", age=25)
    await app.new_event("user.created", ev.model_dump_json().encode())
    await asyncio.sleep(2)

    await app.stop()

    mocked_callback1.assert_awaited()
    mocked_callback1.assert_called_once_with(event=ev)

    mocked_callback2.assert_awaited()
    mocked_callback2.assert_called_once_with(event=ev)


@pytest.mark.asyncio
async def test_retry_event(app):
    async def welcome_email(event: Event): ...

    mocked_callback = create_autospec(welcome_email)
    mocked_callback.side_effect = [KeyError("foo"), None]

    app.on_event(["user.created"], "test_retry_event_use_case", retries=1)(mocked_callback)
    await app.start()

    ev = Event(name="test name", age=25)
    await app.new_event("user.created", ev.model_dump_json().encode())
    await asyncio.sleep(5)

    await app.stop()

    assert len(mocked_callback.mock_calls) == 2


@pytest.mark.asyncio
async def test_dlq_max_retries_event(app):
    async def welcome_email(event: Event): ...

    retries = 3
    mocked_callback = create_autospec(welcome_email)
    mocked_callback.side_effect = [KeyError("foo")] * retries

    app.on_event(["user.created"], "test_dlq_max_retries_event_use_case", retries=retries)(mocked_callback)
    await app.start()

    rabbit_connection = await aio_pika.connect_robust(app.broker_uri)
    channel = await rabbit_connection.channel()
    queue = await channel.get_queue("test_dlq_max_retries_event_use_case.dlq")
    # Check DQL is empty
    with pytest.raises(aio_pika.exceptions.QueueEmpty):
        await queue.get()

    ev = Event(name="test name", age=25)
    await app.new_event("user.created", ev.model_dump_json().encode())
    await asyncio.sleep(10)

    await app.stop()

    message = await queue.get()
    # Mesage in DQL
    assert isinstance(message, aio_pika.IncomingMessage)
    await rabbit_connection.close()
    assert len(mocked_callback.mock_calls) == retries + 1


@pytest.mark.asyncio
async def test_dlq_wrong_cast_event(app):
    async def welcome_email(event: Event): ...

    retries = 3
    mocked_callback = create_autospec(welcome_email)
    mocked_callback.side_effect = [KeyError("foo")] * retries

    app.on_event(["user.created"], "test_dlq_wrong_cast_event", retries=retries)(mocked_callback)
    await app.start()

    rabbit_connection = await aio_pika.connect_robust(app.broker_uri)
    channel = await rabbit_connection.channel()
    queue = await channel.get_queue("test_dlq_wrong_cast_event.dlq")
    # Check DQL is empty
    with pytest.raises(aio_pika.exceptions.QueueEmpty):
        await queue.get()

    event_payload = json.dumps({"no_match_event": 666})
    await app.new_event("user.created", event_payload.encode())
    await asyncio.sleep(10)

    await app.stop()

    message = await queue.get()
    # Mesage in DQL
    assert isinstance(message, aio_pika.IncomingMessage)
    assert "pydantic_core._pydantic_core.ValidationError" in message.headers["exception"]
    await rabbit_connection.close()
    assert len(mocked_callback.mock_calls) == 0
