import requests
from services.minio.storage import save_raw_json
from logger import logger
from sqlalchemy.orm import Session
from datetime import datetime
from services.db.models import RawCurrency
from services.kafka.producer_confluent import send_message
# from services.telergam.alerts import send_alert


def get_currency() -> dict:
    url = "https://www.cbr-xml-daily.ru/daily_json.js"

    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        data = response.json()

        # Добавляем метаданные
        data['_metadata'] = {
            'fetch_timestamp': datetime.now().isoformat(),
            'source': 'cbr_api',
            'data_type': 'raw_currency'
        }

        save_raw_json(bucket="raw-data", prefix="currency", data=data)

        # Отправляем в Kafka
        logger.info("Sending raw currency data to Kafka...")
        success = send_message(
            topic='currencies.raw',
            key=data.get('Date', 'unknown'),
            value=data
        )

        if not success:
            raise Exception("Failed to send data to Kafka")

        logger.info(f"Successfully sent currency data to Kafka for date: {data.get('Date')}")
        # send_alert("✅ Данные о курсах валют успешно отправлены в Kafka")
        return data

    except Exception as e:
        logger.error(f"Ошибка запроса курсов валют: {e}")
        raise


