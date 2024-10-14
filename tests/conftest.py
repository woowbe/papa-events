import pytest
from testcontainers.rabbitmq import RabbitMqContainer

from papa_events import config


@pytest.fixture(scope="session")
def rabbitmq_container():
    config.settings.timeout = 1
    with RabbitMqContainer("rabbitmq:4") as rabbitmq:
        yield rabbitmq
