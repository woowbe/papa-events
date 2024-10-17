import asyncio
import json

import asyncpg
import pydantic
import pytest

from papa_events.application import PapaApp


class Event(pydantic.BaseModel):
    name: str
    age: int


@pytest.mark.asyncio
async def test_failover_event(rabbitmq_container, postgres_container):
    app = PapaApp(
        broker_uri=f"amqp://{rabbitmq_container.get_container_host_ip()}:{rabbitmq_container.get_exposed_port(5672)}",
        failover_uri=postgres_container.get_connection_url().replace("+psycopg2", ""),
    )
    await app.start()

    # Simulate rabbit fail
    rabbitmq_container.exec("rabbitmq-upgrade drain")
    await asyncio.sleep(5)

    ev = Event(name="test name", age=25)
    await app.new_event("user.created", ev.model_dump_json().encode())
    await asyncio.sleep(2)
    await app.stop()

    conn = await asyncpg.connect(dsn=postgres_container.get_connection_url().replace("+psycopg2", ""))
    values = await conn.fetch("SELECT * FROM rabbitmq_failover;")
    await conn.close()

    assert len(values) == 1
    assert json.loads(values[0]["body"]) == ev.model_dump()
    assert values[0]["routing_key"] == "user.created"

    rabbitmq_container.exec("rabbitmq-upgrade revive")
    await asyncio.sleep(5)
