import pytest
from testcontainers.rabbitmq import RabbitMqContainer

@pytest.fixture(scope="session")
def rabbitmq_container():
    with RabbitMqContainer("rabbitmq:4") as rabbitmq:
        yield rabbitmq
