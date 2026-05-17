import requests
from services.minio.storage import save_raw_json
from logger import logger
from sqlalchemy.orm import Session

from services.db.models import RawCurrency

def get_currency() -> dict:
    """
        Получаем курс указанной валюты к рублю по данным ЦБ РФ.
        :param base: Код валюты (например, 'USD', 'EUR'), базовая валюта относительно рубля.
                     Если base='RUB', то курс 1.
        :return: Словарь с ключами:
            - 'timestamp': время получения данных,
            - 'base': указанный код валюты,
            - 'rate': текущий курс валюты к рублю,
            - 'previous': предыдущий курс,
            - 'name': название валюты.
            или None при ошибке.
        """
    url = "https://www.cbr-xml-daily.ru/daily_json.js"

    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()

        save_raw_json(bucket="raw-data", prefix="currency", data=data)
        valutes = data.get('Valute', {})

        return valutes

    except Exception as e:
        logger.error(f"Ошибка запроса курсов валют: {e}")
        return None

def collect_raw_currencies(db: Session, data: dict) -> RawCurrency:
    db_currency = RawCurrency(
        json_data=data
    )
    db.add(db_currency)
    db.commit()
    db.refresh(db_currency)
    return db_currency



# url = "https://www.cbr-xml-daily.ru/daily_json.js"
# filename = "valute_list.json"
# with urllib.request.urlopen(url) as response:
#         content = response.read().decode("utf-8")
# with open(filename, 'w', encoding='utf-8') as f:
#         f.write(content)

