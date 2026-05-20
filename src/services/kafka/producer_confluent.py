from confluent_kafka import Producer
import json
from logger import logger
import socket
import time

# logger = logging.getLogger(__name__)
#
# # Конфигурация producer
# producer_config = {
#     'bootstrap.servers': 'redpanda:9092',
#     'client.id': socket.gethostname(),
#     'acks': '1',
#     'retries': 5,
#     'retry.backoff.ms': 1000,
#     'socket.timeout.ms': 30000,
#     'request.timeout.ms': 30000,
#     'message.timeout.ms': 30000,
#     'delivery.timeout.ms': 60000,
# }
#
# producer = None
#
#
# def delivery_report(err, msg):
#     """Callback для отслеживания доставки сообщений"""
#     if err is not None:
#         logger.error(f'Message delivery failed: {err}')
#     else:
#         logger.info(f'Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}')
#
#
# def create_producer():
#     """Создает producer с проверкой соединения"""
#     global producer
#
#     try:
#         # Проверяем доступность Redpanda
#         sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
#         sock.settimeout(5)
#         result = sock.connect_ex(('redpanda', 9092))
#         sock.close()
#
#         if result != 0:
#             logger.error("Cannot connect to Redpanda:9092")
#             return None
#
#         logger.info("Creating Confluent Kafka producer...")
#         producer = Producer(producer_config)
#
#         # Проверяем соединение — пытаемся получить метаданные
#         metadata = producer.list_topics(timeout=10)
#         logger.info(f"Connected! Available topics: {list(metadata.topics.keys())}")
#
#         return producer
#
#     except Exception as e:
#         logger.error(f"Failed to create producer: {e}")
#         return None
#
#
# def get_producer():
#     """Получает или создает producer"""
#     global producer
#     if producer is None:
#         producer = create_producer()
#     return producer
#
#
# def send_message(topic, key=None, value=None):
#     """Отправляет сообщение в Kafka"""
#     prod = get_producer()
#
#     if prod is None:
#         logger.error("Producer is not initialized")
#         return False
#
#     try:
#         # Преобразуем value в JSON строку
#         if isinstance(value, (dict, list)):
#             value = json.dumps(value, ensure_ascii=False)
#
#         # Преобразуем key в строку
#         if key is not None and not isinstance(key, str):
#             key = str(key)
#
#         logger.info(f"Sending message to {topic}...")
#
#         # Асинхронная отправка
#         prod.produce(
#             topic=topic,
#             key=key,
#             value=value.encode('utf-8') if isinstance(value, str) else value,
#             callback=delivery_report
#         )
#
#         # Ждем отправки всех сообщений
#         prod.flush(timeout=30)
#         logger.info("Message sent successfully")
#         return True
#
#     except Exception as e:
#         logger.error(f"Error sending message: {e}")
#         return False
#
#
# def send_currency_data(data):
#     """Отправляет данные о валютах в Kafka"""
#     import datetime
#
#     # Используем дату как ключ для партиционирования
#     date_str = data.get('Date', datetime.datetime.now().strftime('%Y-%m-%d'))
#
#     return send_message(
#         topic='currencies.raw',
#         key=date_str,
#         value=data
#     )


producer_config = {
    'bootstrap.servers': 'redpanda:9092',
    'client.id': socket.gethostname(),
    'acks': '1',
    'retries': 5,
    'retry.backoff.ms': 1000,
    'socket.timeout.ms': 30000,
    'request.timeout.ms': 30000,
    'message.timeout.ms': 30000,
    'delivery.timeout.ms': 60000,
}

producer = None

def delivery_report(err, msg):
    if err is not None:
        logger.error(f'Доставка сообщения провалена: {err}')
    else:
        logger.info(f'Сообщение доставлено к {msg.topic()} [{msg.partition()}. Offset {msg.offset()}]')

def create_producer():
    global producer

    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(5)
        result = sock.connect_ex(('redpanda', 9092))
        sock.close()

        if result != 0:
            logger.error("Не удалось подключиться к Redpanda:9092")
            return None

        logger.info("Creating Confluent Kafka producer...")
        producer = Producer(producer_config)

        # Проверяем соединение — пытаемся получить метаданные
        metadata = producer.list_topics(timeout=10)
        logger.info(f"Connected! Available topics: {list(metadata.topics.keys())}")

        return producer
    except Exception as e:
        logger.error(f"Failed to create producer: {e}")
        return None


def get_producer():
    """Получает или создает producer"""
    global producer
    if producer is None:
        producer = create_producer()
    return producer


def send_message(topic, key=None, value=None):
    """Отправляет сообщение в Kafka"""
    prod = get_producer()

    if prod is None:
        logger.error("Producer is not initialized")
        return False

    try:
        # Преобразуем value в JSON строку
        if isinstance(value, (dict, list)):
            value = json.dumps(value, ensure_ascii=False)

        # Преобразуем key в строку
        if key is not None and not isinstance(key, str):
            key = str(key)

        logger.info(f"Sending message to {topic}...")

        # Асинхронная отправка
        prod.produce(
            topic=topic,
            key=key,
            value=value.encode('utf-8') if isinstance(value, str) else value,
            callback=delivery_report
        )

        # Ждем отправки всех сообщений
        prod.flush(timeout=30)
        logger.info("Message sent successfully")
        return True

    except Exception as e:
        logger.error(f"Error sending message: {e}")
        return False


def send_currency_data(data):
    """Отправляет данные о валютах в Kafka"""
    import datetime

    # Используем дату как ключ для партиционирования
    date_str = data.get('Date', datetime.datetime.now().strftime('%Y-%m-%d'))

    return send_message(
        topic='currencies.raw',
        key=date_str,
        value=data
    )
