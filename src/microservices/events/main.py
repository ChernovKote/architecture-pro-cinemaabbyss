import asyncio
import json
import logging
import os
from contextlib import asynccontextmanager, suppress
from datetime import datetime, timezone
from typing import Any, Literal
from uuid import uuid4

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.errors import KafkaError
from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pydantic import BaseModel


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

logger = logging.getLogger("events-service")


KAFKA_BROKERS = os.getenv(
    "KAFKA_BROKERS",
    "kafka:9092",
)

MOVIE_EVENTS_TOPIC = "movie-events"
USER_EVENTS_TOPIC = "user-events"
PAYMENT_EVENTS_TOPIC = "payment-events"

EVENT_TOPICS = (
    MOVIE_EVENTS_TOPIC,
    USER_EVENTS_TOPIC,
    PAYMENT_EVENTS_TOPIC,
)


class MovieEvent(BaseModel):
    movie_id: int
    title: str
    action: str
    user_id: int | None = None
    rating: float | None = None
    genres: list[str] | None = None
    description: str | None = None


class UserEvent(BaseModel):
    user_id: int
    action: str
    timestamp: datetime
    username: str | None = None
    email: str | None = None


class PaymentEvent(BaseModel):
    payment_id: int
    user_id: int
    amount: float
    status: str
    timestamp: datetime
    method_type: str | None = None


class Event(BaseModel):
    id: str
    type: str
    timestamp: datetime
    payload: dict[str, Any]


class EventResponse(BaseModel):
    status: Literal["success"]
    partition: int
    offset: int
    event: Event


def serialize_event(event: Event) -> bytes:
    event_data = event.model_dump(
        mode="json",
        exclude_none=True,
    )

    return json.dumps(
        event_data,
        ensure_ascii=False,
    ).encode("utf-8")


async def create_kafka_clients() -> tuple[
    AIOKafkaProducer,
    AIOKafkaConsumer,
]:
    last_error: Exception | None = None

    for attempt in range(1, 21):
        producer = AIOKafkaProducer(
            bootstrap_servers=KAFKA_BROKERS,
            client_id="events-service-producer",
        )

        consumer = AIOKafkaConsumer(
            *EVENT_TOPICS,
            bootstrap_servers=KAFKA_BROKERS,
            client_id="events-service-consumer",
            group_id="events-service",
            auto_offset_reset="earliest",
            enable_auto_commit=True,
        )

        try:
            await producer.start()
            await consumer.start()

            logger.info(
                "Connected to Kafka: brokers=%s",
                KAFKA_BROKERS,
            )

            return producer, consumer

        except Exception as error:
            last_error = error

            with suppress(Exception):
                await consumer.stop()

            with suppress(Exception):
                await producer.stop()

            logger.warning(
                "Kafka is unavailable, attempt %s/20: %s",
                attempt,
                error,
            )

            await asyncio.sleep(3)

    raise RuntimeError(
        "Could not connect to Kafka"
    ) from last_error


async def consume_events(
    consumer: AIOKafkaConsumer,
) -> None:
    logger.info(
        "Kafka consumer started for topics: %s",
        ", ".join(EVENT_TOPICS),
    )

    try:
        async for message in consumer:
            try:
                event = json.loads(
                    message.value.decode("utf-8")
                )

                logger.info(
                    "Event processed: "
                    "topic=%s partition=%s offset=%s event=%s",
                    message.topic,
                    message.partition,
                    message.offset,
                    event,
                )

            except (UnicodeDecodeError, json.JSONDecodeError):
                logger.exception(
                    "Could not decode event: "
                    "topic=%s partition=%s offset=%s",
                    message.topic,
                    message.partition,
                    message.offset,
                )

    except asyncio.CancelledError:
        logger.info("Kafka consumer task stopped")
        raise


@asynccontextmanager
async def lifespan(app: FastAPI):
    producer, consumer = await create_kafka_clients()

    app.state.kafka_producer = producer
    app.state.kafka_consumer = consumer

    consumer_task = asyncio.create_task(
        consume_events(consumer),
        name="events-consumer",
    )

    try:
        yield
    finally:
        consumer_task.cancel()

        with suppress(asyncio.CancelledError):
            await consumer_task

        await consumer.stop()
        await producer.stop()

        logger.info("Kafka clients stopped")


app = FastAPI(
    title="CinemaAbyss Events Service",
    lifespan=lifespan,
)


@app.exception_handler(RequestValidationError)
async def validation_error_handler(
    request: Request,
    error: RequestValidationError,
) -> JSONResponse:
    return JSONResponse(
        status_code=400,
        content={"error": "Invalid request body"},
    )


@app.exception_handler(KafkaError)
async def kafka_error_handler(
    request: Request,
    error: KafkaError,
) -> JSONResponse:
    logger.exception(
        "Kafka operation failed",
        exc_info=error,
    )

    return JSONResponse(
        status_code=500,
        content={"error": "Kafka operation failed"},
    )


@app.get("/api/events/health")
async def health() -> dict[str, bool]:
    return {"status": True}


async def publish_event(
    request: Request,
    topic: str,
    event_type: str,
    payload: BaseModel,
) -> EventResponse:
    event = Event(
        id=str(uuid4()),
        type=event_type,
        timestamp=datetime.now(timezone.utc),
        payload=payload.model_dump(
            mode="json",
            exclude_none=True,
        ),
    )

    metadata = await request.app.state.kafka_producer.send_and_wait(
        topic,
        serialize_event(event),
    )

    logger.info(
        "Event published: topic=%s partition=%s "
        "offset=%s event_id=%s",
        topic,
        metadata.partition,
        metadata.offset,
        event.id,
    )

    return EventResponse(
        status="success",
        partition=metadata.partition,
        offset=metadata.offset,
        event=event,
    )


@app.post(
    "/api/events/movie",
    response_model=EventResponse,
    status_code=201,
)
async def create_movie_event(
    payload: MovieEvent,
    request: Request,
) -> EventResponse:
    return await publish_event(
        request=request,
        topic=MOVIE_EVENTS_TOPIC,
        event_type="movie",
        payload=payload,
    )


@app.post(
    "/api/events/user",
    response_model=EventResponse,
    status_code=201,
)
async def create_user_event(
    payload: UserEvent,
    request: Request,
) -> EventResponse:
    return await publish_event(
        request=request,
        topic=USER_EVENTS_TOPIC,
        event_type="user",
        payload=payload,
    )


@app.post(
    "/api/events/payment",
    response_model=EventResponse,
    status_code=201,
)
async def create_payment_event(
    payload: PaymentEvent,
    request: Request,
) -> EventResponse:
    return await publish_event(
        request=request,
        topic=PAYMENT_EVENTS_TOPIC,
        event_type="payment",
        payload=payload,
    )