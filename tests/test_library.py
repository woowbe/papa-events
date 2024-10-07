import pytest
import asyncio
import pydantic
from papa_events.application import PapaApp

@pytest.mark.asyncio(loop_scope="module")
async def test_wip(rabbitmq_container):
    class Event(pydantic.BaseModel):
        name: str
        age: int
    async def sample_callback(event: Event):
        print(event.name)
    app = PapaApp(broker_uri=f"amqp://{rabbitmq_container.get_container_host_ip()}:{rabbitmq_container.get_exposed_port(5672)}")
    app.on_event(["user.created"], "register_user_on_user_created")(sample_callback)
    await app.start()
    ev = Event(name="test name", age=25)
    await app.new_event("user.created", ev.model_dump_json().encode())
    await asyncio.sleep(10)
    await app.stop()