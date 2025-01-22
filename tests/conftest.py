import pytest
from testcontainers.postgres import PostgresContainer
from testcontainers.rabbitmq import RabbitMqContainer

from papa_events import config
from papa_events.application import PapaApp


@pytest.fixture(scope="session")
def rabbitmq_container():
    config.settings.timeout = 1
    with RabbitMqContainer("rabbitmq:4" if config.settings.rabbitmq_version == 4 else "rabbitmq:3") as rabbitmq:
        yield rabbitmq


@pytest.fixture(scope="session")
def postgres_container():
    with PostgresContainer("postgres:17.2", username="test", password="test", dbname="test") as postgres:
        yield postgres


@pytest.fixture(scope="function")
def app(rabbitmq_container):
    broker_uri = f"amqp://{rabbitmq_container.get_container_host_ip()}:{rabbitmq_container.get_exposed_port(5672)}"
    return PapaApp(broker_uri=broker_uri)
