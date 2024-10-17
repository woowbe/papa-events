import asyncio
from unittest.mock import create_autospec

import aio_pika
import pydantic
import pytest
from pydantic import BaseModel


class Event(pydantic.BaseModel):
    name: str
    age: int


@pytest.mark.asyncio
async def test_publish_event(app):
    async def welcome_email(event: Event): ...

    class CustomEvent(BaseModel):
        user: int

    ev2 = CustomEvent(user=999)

    async def activate_account(event: CustomEvent): ...

    mocked_callback = create_autospec(welcome_email)
    mocked_callback.return_value = [
        {"name": "email.sended", "payload": ev2.model_dump()},
        {"name": "email.sended", "payload": ev2},
    ]
    mocked_callback2 = create_autospec(activate_account)

    app.on_event(["user.created"], "test_publish_event_use_case")(mocked_callback)
    app.on_event(["email.*"], "test_publish_event_use_case2")(mocked_callback2)

    await app.start()
    ev = Event(name="test name", age=25)
    await app.new_event("user.created", ev.model_dump_json().encode())
    await asyncio.sleep(5)
    await app.stop()

    mocked_callback.assert_awaited()
    mocked_callback.assert_called_once_with(event=ev)

    mocked_callback2.assert_awaited()
    mocked_callback2.assert_called_with(event=ev2)
    assert mocked_callback2.call_count == 2


@pytest.mark.asyncio
async def test_publish_event_no_object(app):
    async def welcome_email(event: Event): ...

    class CustomEvent(BaseModel):
        user: int

    async def activate_account(event: CustomEvent): ...

    mocked_callback = create_autospec(welcome_email)
    mocked_callback.return_value = [
        {"name": "email.sended", "payload": "no_object"},
    ]
    mocked_callback2 = create_autospec(activate_account)

    app.on_event(["user.created"], "test_publish_event_no_object")(mocked_callback)
    app.on_event(["email.*"], "test_publish_event_no_object2")(mocked_callback2)

    await app.start()
    ev = Event(name="test name", age=25)
    await app.new_event("user.created", ev.model_dump_json().encode())
    await asyncio.sleep(2)
    await app.stop()

    rabbit_connection = await aio_pika.connect_robust(app.broker_uri)
    channel = await rabbit_connection.channel()
    queue = await channel.get_queue("test_publish_event_no_object.dlq")
    message = await queue.get()
    await rabbit_connection.close()

    mocked_callback.assert_awaited()
    assert "payload must be jsonable object" in message.headers["exception"]
    mocked_callback.assert_called_once_with(event=ev)

    assert mocked_callback2.call_count == 0


@pytest.mark.asyncio
async def test_publish_event_no_jsonable(app):
    async def welcome_email(event: Event): ...

    class CustomEvent(BaseModel):
        user: int

    async def activate_account(event: CustomEvent): ...

    class NOJSONABLE: ...

    a = NOJSONABLE()
    mocked_callback = create_autospec(welcome_email)
    mocked_callback.return_value = [
        {"name": "email.sended", "payload": {"user": a}},
    ]
    mocked_callback2 = create_autospec(activate_account)

    app.on_event(["user.created"], "test_publish_event_no_jsonable")(mocked_callback)
    app.on_event(["email.*"], "test_publish_event_no_jsonable2")(mocked_callback2)

    await app.start()
    ev = Event(name="test name", age=25)
    await app.new_event("user.created", ev.model_dump_json().encode())
    await asyncio.sleep(2)
    await app.stop()

    rabbit_connection = await aio_pika.connect_robust(app.broker_uri)
    channel = await rabbit_connection.channel()
    queue = await channel.get_queue("test_publish_event_no_jsonable.dlq")
    message = await queue.get()
    await rabbit_connection.close()

    mocked_callback.assert_awaited()
    assert "payload is not JSONable" in message.headers["exception"]
    mocked_callback.assert_called_once_with(event=ev)
    assert mocked_callback2.call_count == 0
