import asyncio
from unittest.mock import create_autospec

import pydantic
import pytest

from papa_events.application import PapaApp


class Event(pydantic.BaseModel):
    name: str
    age: int


@pytest.mark.asyncio
async def test_simple_event(rabbitmq_container):
    async def welcome_email(event: Event): ...

    mocked_callback = create_autospec(welcome_email)
    app = PapaApp(
        broker_uri=f"amqp://{rabbitmq_container.get_container_host_ip()}:{rabbitmq_container.get_exposed_port(5672)}",
    )
    app.on_event(["user.created"], "test_simple_event_use_case")(mocked_callback)

    await app.start()
    ev = Event(name="test name", age=25)
    await app.new_event("user.created", ev.model_dump_json().encode())
    await asyncio.sleep(2)
    await app.stop()

    mocked_callback.assert_awaited()
    mocked_callback.assert_called_once_with(event=ev)


@pytest.mark.asyncio
async def test_multiple_events(rabbitmq_container):
    async def welcome_email(event: Event): ...

    async def register_analitics(event: Event): ...

    mocked_callback1 = create_autospec(welcome_email)
    mocked_callback2 = create_autospec(register_analitics)
    app = PapaApp(
        broker_uri=f"amqp://{rabbitmq_container.get_container_host_ip()}:{rabbitmq_container.get_exposed_port(5672)}"
    )
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
async def test_retry_event(rabbitmq_container):
    async def welcome_email(event: Event): ...

    mocked_callback = create_autospec(welcome_email)
    mocked_callback.side_effect = [KeyError("foo"), None]
    app = PapaApp(
        broker_uri=f"amqp://{rabbitmq_container.get_container_host_ip()}:{rabbitmq_container.get_exposed_port(5672)}"
    )
    app.on_event(["user.created"], "test_retry_event_use_case", retries=1)(mocked_callback)

    await app.start()
    ev = Event(name="test name", age=25)
    await app.new_event("user.created", ev.model_dump_json().encode())
    await asyncio.sleep(5)
    await app.stop()

    assert len(mocked_callback.mock_calls) == 2
