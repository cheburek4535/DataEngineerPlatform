from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable
import json
from logger import logger
import time

# def read_currency():
#     consumer = KafkaConsumer(
#         'currencies.raw',
#         bootstrap_servers=['redpanda:9092'],
#         auto_offset_reset='earliest',
#         value_deserializer=lambda v: json.loads(v.decode('utf-8')),
#     )
#     for message in consumer:
#         print(message.value)
#

def create_consumer(topic="currencies.raw", group_id="currency_processor"):
    retries = 5
    delay = 10

    for attempt in range(retries):
        try:
            consumer = KafkaConsumer(
                topic,
                bootstrap_servers=['redpanda:9092'],
                auto_offset_reset='earliest',
                value_deserializer=lambda v: json.loads(v.decode('utf-8')),
                group_id=group_id,
                session_timeout_ms=30000,
                request_timeout_ms=40000,
                consumer_timeout_ms=10000
            )
            logger.info(f"Consumer успешно подключен к топику {topic}")
            return consumer

        except NoBrokersAvailable as e:
            logger.warning(f"Попытка {attempt + 1}/{retries}: Не удалось подключиться к consumer. Переподключение...")
            if attempt < retries - 1:
                time.sleep(delay)
            else:
                raise Exception(f"Failed to create consumer after {retries} attempts") from e


def read_currency(max_messages=None, timeout_ms=10000):
    """Читает сообщения из Kafka с ограничением по времени/количеству"""
    try:
        consumer = create_consumer()
        if not consumer:
            return

        message_count = 0
        start_time = time.time()

        for message in consumer:
            print(f"Получено: {message.value}")
            message_count += 1

            # Выходим по условию
            if max_messages and message_count >= max_messages:
                break
            if timeout_ms and (time.time() - start_time) * 1000 > timeout_ms:
                break

    except Exception as e:
        logger.error(f"Ошибка чтения Kafka: {e}")
    finally:
        if 'consumer' in locals():
            consumer.close()
