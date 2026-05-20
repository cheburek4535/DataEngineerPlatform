# import time
#
# from kafka import KafkaProducer
# import json
# from logger import logger
# from kafka.errors import NoBrokersAvailable
#
# # producer = KafkaProducer(
# #     bootstrap_servers=['redpanda:9092'],
# #     value_serializer=lambda v: json.dumps(v).encode('utf-8')
# # )
#
# def create_producer(retries=5, delay=10):
#     for attempt in range(retries):
#         try:
#             producer = KafkaProducer(
#                 bootstrap_servers=['redpanda:9092'],
#                 value_serializer=lambda v: json.dumps(v).encode('utf-8'),
#                 request_timeout_ms=30000,
#                 api_version_auto_timeout_ms=30000,
#                 max_block_ms=30000,
#                 retries=3,
#                 acks='all'
#             )
#             producer.bootstrap_connected()
#             logger.info("Успешное подключение к Redpanda")
#             return producer
#         except NoBrokersAvailable as e:
#             logger.warning(f"Попытка {attempt + 1}/{retries}: Не удалось подключиться к Redpanda. Переподключение через {delay} секунд...")
#             if attempt < retries - 1:
#                 time.sleep(delay)
#             else:
#                 raise Exception(f"Failed to connect to Redpanda after {retries} attempts") from e
#
#
# try:
#     producer = create_producer()
# except Exception as e:
#     logger.error(f"Failed to initialize Kafka producer: {e}")
#     producer = None


from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable, KafkaTimeoutError
import json
import time
import logging
import socket

logger = logging.getLogger(__name__)


def check_connectivity(host, port):
    """Проверяет доступность хоста"""
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(5)
        result = sock.connect_ex((host, port))
        sock.close()
        return result == 0
    except Exception as e:
        logger.error(f"Socket check failed: {e}")
        return False


def create_producer(retries=10, delay=10):
    """Создает producer с повторными попытками и детальной диагностикой"""

    logger.info(f"Attempting to connect to Redpanda at redpanda:9092")

    # Проверяем DNS и сетевую доступность
    for attempt in range(retries):
        logger.info(f"Connection attempt {attempt + 1}/{retries}")

        # Проверяем DNS
        try:
            ip = socket.gethostbyname('redpanda')
            logger.info(f"Resolved redpanda to IP: {ip}")
        except socket.gaierror as e:
            logger.error(f"DNS resolution failed: {e}")
            time.sleep(delay)
            continue

        # Проверяем TCP подключение
        if not check_connectivity('redpanda', 9092):
            logger.error(f"Cannot establish TCP connection to redpanda:9092")
            time.sleep(delay)
            continue

        logger.info(f"TCP connection to redpanda:9092 successful")

        # Пробуем создать producer
        try:
            producer = KafkaProducer(
                bootstrap_servers=['redpanda:9092'],
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                key_serializer=lambda k: k.encode('utf-8') if k else None,
                # Увеличиваем таймауты
                request_timeout_ms=60000,
                api_version_auto_timeout_ms=60000,
                max_block_ms=60000,
                connections_max_idle_ms=60000,
                # Критические настройки
                retries=5,
                acks=1,  # Ждем подтверждения только от лидера
                compression_type=None,  # Без сжатия для отладки
                # Отключаем проверку версии API (иногда помогает с Redpanda)
                api_version=(0, 10, 2),  # Фиксированная версия API
            )

            # Проверяем подключение
            if producer.bootstrap_connected():
                logger.info("Successfully connected to Redpanda!")

                # Пробуем отправить тестовое сообщение
                test_future = producer.send('currencies.raw', {'test': 'connection'})
                test_result = test_future.get(timeout=10)
                logger.info(f"Test message sent successfully to partition {test_result.partition}")

                return producer
            else:
                logger.error("Producer created but not connected")

        except Exception as e:
            logger.error(f"Failed to create producer: {type(e).__name__}: {e}")

        if attempt < retries - 1:
            logger.info(f"Retrying in {delay} seconds...")
            time.sleep(delay)

    raise Exception(f"Failed to connect to Redpanda after {retries} attempts")


# Глобальный producer
producer = None


def get_producer():
    """Ленивая инициализация producer"""
    global producer
    if producer is None:
        producer = create_producer()
    return producer


def send_currency_data(data):
    """Отправляет данные о валютах в Kafka"""
    prod = get_producer()

    logger.info(f"Sending data to currencies.raw: {json.dumps(data, ensure_ascii=False)[:200]}...")

    try:
        future = prod.send(
            'currencies.raw',
            value=data,
            key=str(data.get('Date', 'unknown'))  # Используем дату как ключ
        )

        # Ждем подтверждения
        record_metadata = future.get(timeout=10)
        logger.info(f"Message sent to partition {record_metadata.partition}, offset {record_metadata.offset}")
        return True

    except KafkaTimeoutError as e:
        logger.error(f"Timeout sending message: {e}")
        # Пробуем пересоздать producer
        global producer
        producer = None
        return False
    except Exception as e:
        logger.error(f"Error sending message: {type(e).__name__}: {e}")
        return False