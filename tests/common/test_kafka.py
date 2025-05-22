import json
import pytest
import asyncio

from mm_bronze.common.kafka import (
    init_async_producer,
    close_async_producer,
    get_async_producer,
    init_async_consumer,
    close_async_consumer,
    get_async_consumer,
    _get_avail_consumer_groups,
)


TEST_TOPIC = "test-topic"
TEST_GROUP = "test-group"
TEST_MESSAGE = {"foo": "bar"}


@pytest.mark.asyncio
async def test_kafka_produce_consume():
    await init_async_producer()
    producer = get_async_producer()

    payload = json.dumps(TEST_MESSAGE).encode("utf-8")

    await producer.send_and_wait(TEST_TOPIC, payload)

    await init_async_consumer(TEST_GROUP, TEST_TOPIC)
    consumer = get_async_consumer(TEST_GROUP)
    groups = _get_avail_consumer_groups()
    # should be a single group
    assert [g for g in groups] == [TEST_GROUP]

    try:
        async with asyncio.timeout(3):
            async for msg in consumer:
                print(
                    "consumed: ",
                    msg.topic,
                    msg.partition,
                    msg.offset,
                    msg.key,
                    msg.value,
                    msg.timestamp,
                )
                await consumer.commit()
                # msg.value is a bytes type
                assert isinstance(msg.value, bytes)
                assert msg.value == payload
    except TimeoutError:
        pass

    finally:
        await close_async_consumer(TEST_GROUP)
        await close_async_producer()

    print("Great success!")


async def _debug():
    await init_async_producer()
    producer = get_async_producer()

    payload = json.dumps(TEST_MESSAGE).encode("utf-8")

    await producer.send_and_wait(TEST_TOPIC, payload)

    await init_async_consumer(TEST_GROUP, TEST_TOPIC)
    consumer = get_async_consumer(TEST_GROUP)
    print(_get_avail_consumer_groups())

    try:
        async with asyncio.timeout(10):
            async for msg in consumer:
                print(
                    "consumed: ",
                    msg.topic,
                    msg.partition,
                    msg.offset,
                    msg.key,
                    msg.value,
                    msg.timestamp,
                )
                print(type(msg.value))
                await consumer.commit()
    except TimeoutError:
        pass

    finally:
        await close_async_consumer(TEST_GROUP)
        await close_async_producer()

    print("Great success!")


if __name__ == "__main__":
    asyncio.run(_debug(), debug=True)
