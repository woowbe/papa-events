import asyncio
import logging
import signal
from collections.abc import Callable
from functools import partial
from inspect import Signature, signature

import aio_pika
import pydantic
from aio_pika import ExchangeType
from opentelemetry.propagate import extract, get_global_textmap
from opentelemetry.trace import get_tracer_provider, set_span_in_context

from .config import settings
from .event import CallBack, UseCase

HANDLED_SIGNALS = (
    signal.SIGINT,  # Unix signal 2. Sent by Ctrl+C.
    signal.SIGTERM,  # Unix signal 15. Sent by `kill <pid>`.
)

logger = logging.getLogger(__name__)


class PapaApp:
    def __init__(self, broker_uri: str, max_jobs: int = settings.max_jobs):
        self.broker_uri = broker_uri
        self.use_cases: dict[str, UseCase] = {}
        self.connection: aio_pika.abc.AbstractRobustConnection | None = None
        self.channel: aio_pika.abc.AbstractChannel | None = None
        self.default_exchange: aio_pika.abc.AbstractExchange | None = None
        self.retry_exchange: aio_pika.abc.AbstractExchange | None = None
        self.dlq_exchange: aio_pika.abc.AbstractExchange | None = None
        self.consumers = []
        self.max_jobs: int = max_jobs
        self.tracer = get_tracer_provider().get_tracer("papa-events")

    def on_event(self, event_names: list[str], use_case_name: str, retries: int = settings.retries) -> Callable:
        def decorator(func: Callable) -> Callable:
            if use_case_name in self.use_cases:
                raise Exception(f"Ya tiene una funcion para el caso de uso <{use_case_name}>")
            sig = signature(func)
            kws = [
                (name, parameter.annotation)
                for name, parameter in sig.parameters.items()
                if parameter.annotation not in [Signature.empty]
            ]
            # if len(kws) != 1:
            #     raise Exception("Tienes que tener un parametro con tipo para el evento")
            param_name, param_model = kws[0]
            if use_case_name not in self.use_cases:
                callback_config = CallBack(function=func, param_name=param_name, param_model=param_model)
                self.use_cases[use_case_name] = UseCase(
                    name=use_case_name,
                    callback=callback_config,
                    event_names=[],
                    retries=retries,
                )
            self.use_cases[use_case_name].event_names.extend(event_names)
            return func

        return decorator

    async def _job_processor(self, message: aio_pika.abc.AbstractIncomingMessage, use_case: UseCase) -> None:
        if message.routing_key is None or self.dlq_exchange is None:
            raise Exception("Init needed")
        logger.warning(f"Procesando mensaje #{message.message_id} en {use_case.name}")
        ctx = extract(message.headers)
        with self.tracer.start_as_current_span(f"consumer:{use_case.name}", context=ctx):
            try:
                kwargs = {use_case.callback.param_name: use_case.callback.param_model.model_validate_json(message.body)}
                await use_case.callback.function(**kwargs)
            except pydantic.ValidationError:
                logger.error(f"Decode error, message #{ message.message_id } to DLQ")
                await self.dlq_exchange.publish(message=message, routing_key=message.routing_key)
                await message.ack()
            except Exception as exc:
                retried_times: int = message.headers.get("x-delivery-count", 0)
                if (
                    retried_times >= use_case.retries  # type: ignore
                ):
                    logger.error(f"Max retries ({ use_case.retries }) for message #{message.message_id} to DLQ")
                    message.headers["exception"] = str(exc)
                    await self.dlq_exchange.publish(message=message, routing_key=message.routing_key)
                    await message.ack()
                else:
                    logger.warning(
                        f"Retry message {str(exc)} #{ message.message_id } @ { use_case.name } ({retried_times + 1})"
                    )
                    await message.reject()
            else:
                await message.ack()

    async def start(self) -> None:
        self.connection = await aio_pika.connect_robust(self.broker_uri)
        self.channel = await self.connection.channel()
        await self.channel.set_qos(prefetch_count=self.max_jobs)
        self.default_exchange = await self.channel.declare_exchange(name="domain_events", type=ExchangeType.TOPIC)
        self.retry_exchange = await self.channel.declare_exchange(name="domain_events.retry", type=ExchangeType.DIRECT)
        self.dlq_exchange = await self.channel.declare_exchange(name="domain_events.dlq", type=ExchangeType.DIRECT)
        for use_case in self.use_cases:
            default_queue = await self.channel.declare_queue(
                name=use_case,
                durable=True,
                arguments={
                    "x-dead-letter-exchange": "domain_events.retry",
                    "x-dead-letter-routing-key": use_case,
                    "x-queue-type": "quorum",
                },
            )
            await default_queue.bind(exchange=self.default_exchange, routing_key=use_case)
            for event_name in self.use_cases[use_case].event_names:
                await default_queue.bind(exchange=self.default_exchange, routing_key=event_name)
            retry_queue = await self.channel.declare_queue(
                name=f"{use_case}.retry",
                durable=True,
                arguments={
                    "x-dead-letter-exchange": "domain_events",
                    "x-message-ttl": 10000,
                    "x-dead-letter-routing-key": use_case,
                    "x-queue-type": "quorum",
                },
            )
            await retry_queue.bind(exchange=self.retry_exchange, routing_key=use_case)
            dead_letter_queue = await self.channel.declare_queue(
                name=f"{use_case}.dlq",
                durable=True,
                arguments={"x-queue-type": "quorum"},
            )
            await dead_letter_queue.bind(exchange=self.dlq_exchange, routing_key=use_case)
            self.consumers.append(
                (
                    await default_queue.consume(
                        callback=partial(self._job_processor, use_case=self.use_cases[use_case])
                    ),
                    default_queue,
                )
            )

    async def stop(self) -> None:
        if self.connection is None:
            raise Exception("Init needed")
        async with asyncio.TaskGroup() as tg:
            for consumer, queue in self.consumers:
                print(f"Parando cola {consumer}")
                tg.create_task(queue.cancel(consumer))
        await self.connection.close()

    async def new_event(self, event_name: str, payload: bytes) -> None:
        if self.default_exchange is None:
            raise Exception("Init needed")
        headers: aio_pika.abc.HeadersType = {}
        with self.tracer.start_as_current_span(f"publisher:{event_name}") as current_span:
            propagator = get_global_textmap()
            if propagator:
                propagator.inject(headers, set_span_in_context(current_span))
            await self.default_exchange.publish(
                message=aio_pika.Message(body=payload, headers=headers), routing_key=event_name
            )
