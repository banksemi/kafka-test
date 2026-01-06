import json

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
import asyncio
import time

published_index = 0
consumed_index = 0

succeed = 0
failed = 0

publishing_delay_seconds = 1
ready_consumer = asyncio.Event()

TOPIC_NAME = f'test_topic'
BOOTSTRAP_SERVERS = '127.0.0.1:29092,127.0.0.1:39092,127.0.0.1:49092'

async def consume():
    global consumed_index, succeed, failed
    consumer = AIOKafkaConsumer(
        TOPIC_NAME,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        # auto_offset_reset='earliest',
    )
    await consumer.start()
    ready_consumer.set()
    try:
        async for msg_bytes in consumer:
            message = json.loads(msg_bytes.value.decode('utf-8'))
            message_index = message['index']
            message_timestamp = message['timestamp']
            if message_index == consumed_index:
                succeed += 1
            else:
                failed += 1
            print(f'received {message_index} (expected {consumed_index}, rtt: {time.time() - message_timestamp:.3f}s):')
            consumed_index += 1
            print(f'consumed {consumed_index} messages, succeed: {succeed}, failed: {failed}')
    finally:
        await consumer.stop()

async def produce():
    global published_index
    producer = AIOKafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        acks=1,
    )
    await producer.start()
    await ready_consumer.wait() # consumer가 준비되었을 때 0번 메시지부터 전송
    try:
        while True:
            print(f'publish {published_index}')
            message = {
                'dummy' : 'a' * 1024,
                'index': published_index,
                'timestamp': time.time(),
            }
            await producer.send_and_wait(
                TOPIC_NAME,
                bytes(json.dumps(message), 'utf-8'),)
            published_index += 1
            await asyncio.sleep(publishing_delay_seconds)
    finally:
        await producer.stop()

async def main():
    print(f"Topic: {TOPIC_NAME}, Bootstrap Servers: {BOOTSTRAP_SERVERS}")
    functions = []
    functions.append(consume())
    # ready_consumer.set()
    functions.append(produce())
    await asyncio.gather(*functions)
if __name__ == '__main__':
    asyncio.run(main())