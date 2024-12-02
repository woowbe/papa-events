import asyncio
import logging
import signal
import time
import traceback
from collections.abc import Callable
from functools import partial
from inspect import Signature, signature

import aio_pika
import asyncpg
import pydantic
from aio_pika import ExchangeType
from aio_pika.abc import AbstractQueue, ConsumerTag
from opentelemetry.propagate import extract, get_global_textmap
from opentelemetry.trace import get_tracer_provider, set_span_in_context
from pydantic import BaseModel
from pydantic_core import to_json

from .config import settings
from .event import CallBack, OutputEvent, UseCase
from .exceptions import PapaException

HANDLED_SIGNALS = (
    signal.SIGINT,  # Unix signal 2. Sent by Ctrl+C.
    signal.SIGTERM,  # Unix signal 15. Sent by `kill <pid>`.
)


class PapaApp:
    def __init__(
        self,
        broker_uri: str,
        failover_uri: str | None = None,
        max_jobs: int = settings.max_jobs,
        logger: logging.Logger | None = None,
    ):
        self.logger = logger if logger else logging.getLogger(__name__)
        self.broker_uri = broker_uri
        self.failover_uri = failover_uri
        self.failover_pool = None
        self.use_cases: dict[str, UseCase] = {}
        self.connection: aio_pika.abc.AbstractRobustConnection | None = None
        self.channel: aio_pika.abc.AbstractChannel | None = None
        self.default_exchange: aio_pika.abc.AbstractExchange | None = None
        self.retry_exchange: aio_pika.abc.AbstractExchange | None = None
        self.dlq_exchange: aio_pika.abc.AbstractExchange | None = None
        self.consumers: list[tuple[ConsumerTag, AbstractQueue]] = []
        self.max_jobs: int = max_jobs
        self.tracer = get_tracer_provider().get_tracer("papa-events")
        self.opentelemetry_propagator = get_global_textmap()

    def on_event(self, event_names: list[str], use_case_name: str, retries: int = settings.retries) -> Callable:
        def decorator(func: Callable) -> Callable:
            if use_case_name in self.use_cases:
                raise PapaException(f"Duplicate functions for <{use_case_name}>")
            sig = signature(func)
            kws = [
                (name, parameter.annotation)
                for name, parameter in sig.parameters.items()
                if parameter.annotation not in [Signature.empty] and issubclass(parameter.annotation, BaseModel)
            ]
            # Only allow one BaseModel instance for event
            if len(kws) != 1 or not issubclass(kws[0][1], pydantic.BaseModel):
                raise PapaException("You need one pydantic.BaseModel function param")
            has_name_param = any(True for name, parameter in sig.parameters.items() if name == "event_name")
            # event_name is needed
            if not has_name_param:
                raise PapaException("You need one 'event_name' function param")
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
        ctx = extract(message.headers)
        with self.tracer.start_as_current_span(f"consumer:{use_case.name}", context=ctx) as current_span:
            self.logger.info(f"Processing message for {use_case.name} <{message.message_id}>")
            # Cast the message body to event model
            try:
                kwargs = {
                    use_case.callback.param_name: use_case.callback.param_model.model_validate_json(message.body),
                    "event_name": message.routing_key,
                }
            except pydantic.ValidationError:
                self.logger.exception(
                    f"DLQ: The model for {use_case.name} do not validate for incoming message <{ message.message_id }>"
                )
                message.headers["exception"] = traceback.format_exc(chain=False)
                await self.dlq_exchange.publish(message=message, routing_key=use_case.name)
                await message.ack()
                return

            # Execute the function with the custom event model
            try:
                response: list | None = await use_case.callback.function(**kwargs)
            except Exception as exc:
                retried_times: int = message.headers.get("x-delivery-count", 0)
                if retried_times >= use_case.retries:
                    self.logger.exception(
                        f"DLQ: Max retries ({ use_case.retries }) for {use_case.name} <{ message.message_id }>"
                    )
                    message.headers["exception"] = traceback.format_exc(chain=False)
                    await self.dlq_exchange.publish(message=message, routing_key=use_case.name)
                    await message.ack()
                else:
                    self.logger.warning(
                        f"RETRY: {retried_times + 1}/{use_case.retries} "
                        f"for {use_case.name} <{ message.message_id }> exc: {str(exc)}"
                    )
                    await message.reject()
                return

            # Prepare response for publishing
            try:
                response_events = [OutputEvent.model_validate(item) for item in response] if response else []
            except pydantic.ValidationError:
                self.logger.exception(
                    f"DLQ: pydantic.ValidationError output event error for {use_case.name} <{ message.message_id }>"
                )
                message.headers["exception"] = traceback.format_exc(chain=False)
                await self.dlq_exchange.publish(message=message, routing_key=use_case.name)
                response_events = []

            # Publish events
            if response_events:
                async with asyncio.TaskGroup() as tg:
                    for event in response_events:
                        output_event = aio_pika.Message(body=event.payload)
                        if self.opentelemetry_propagator:
                            self.opentelemetry_propagator.inject(
                                output_event.headers, set_span_in_context(current_span)
                            )
                        tg.create_task(self.default_exchange.publish(message=output_event, routing_key=event.name))
            await message.ack()

    async def start(self) -> None:
        self.connection = await aio_pika.connect_robust(self.broker_uri)
        if self.failover_uri:
            self.failover_pool = await asyncpg.create_pool(dsn=self.failover_uri)
            async with self.failover_pool.acquire() as connection:
                await connection.execute("""
                    CREATE TABLE IF NOT EXISTS rabbitmq_failover(
                        id serial PRIMARY KEY,
                        timestamp int,
                        message_id text NULL,
                        exchange text,
                        routing_key text,
                        body jsonb,
                        headers jsonb
                    );
                """)
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
                    "x-message-ttl": settings.timeout * 1000,
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
            raise PapaException("Init needed")
        async with asyncio.TaskGroup() as tg:
            for consumer, queue in self.consumers:
                self.logger.info(f"Stopping tag {consumer}")
                tg.create_task(queue.cancel(consumer))
        if self.failover_pool:
            await self.failover_pool.close()
        await self.connection.close()

    async def new_event(self, event_name: str, payload: bytes | BaseModel) -> None:
        if self.default_exchange is None:
            raise PapaException("Init needed")
        if isinstance(payload, BaseModel):
            payload = payload.model_dump_json().encode()
        message = aio_pika.Message(body=payload)
        with self.tracer.start_as_current_span(f"publisher:{event_name}") as current_span:
            if self.opentelemetry_propagator:
                self.opentelemetry_propagator.inject(message.headers, set_span_in_context(current_span))
            try:
                await self.default_exchange.publish(message=message, routing_key=event_name)
            except Exception:
                self.logger.exception("Domain events in failover mode")
                if self.failover_pool:
                    async with self.failover_pool.acquire() as connection:
                        # Open a transaction.
                        await connection.execute(
                            """
                            INSERT INTO rabbitmq_failover(timestamp, message_id, exchange, routing_key, body, headers)
                            VALUES ($1, $2, $3, $4, $5, $6);
                        """,
                            time.time(),
                            message.message_id if message.message_id else None,
                            self.default_exchange.name,
                            event_name,
                            message.body.decode(),
                            to_json(message.headers).decode(),
                        )
