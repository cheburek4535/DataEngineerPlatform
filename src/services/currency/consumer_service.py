import json
from datetime import datetime, timezone, timedelta
from decimal import Decimal
from typing import Optional, Dict

from sqlalchemy.orm import Session
from sqlalchemy import and_

from services.db.models import RawCurrency, Currency, CurrencyHistory, CurrencySharpChange
from services.db.db import get_session
from logger import logger
from services.telegram.alerts import send_alert_sync


def process_raw_currency_message(message_value: dict) -> bool:
    db = get_session()

    try:
        logger.info(f"Processing currency data for date: {message_value.get('Date', 'unknown')}")

        # Извлекаем данные о валютах
        valutes = message_value.get('Valute', {})

        if not valutes:
            logger.warning("No 'Valute' data in message")
            return False

        raw_currency = save_raw_currency_to_db(db, valutes)

        currencies = save_structured_currencies(db, raw_currency)

        anomalies = check_currency_anomalies(db, currencies)

        if anomalies:
            logger.warning(f"Found {len(anomalies)} currency anomalies!")
            for code, anomaly in anomalies.items():
                logger.warning(f"  - {code}: {anomaly.change_percents:.2f}% change")
        else:
            logger.info("No currency anomalies detected")

        db.commit()
        logger.info("✅ Successfully processed currency data")
        send_alert_sync("✅ Данные о курсах валют успешно обработаны")
        return True
    except Exception as e:
        db.rollback()
        logger.error(f"Error processing currency message: {e}")
        return False
    finally:
        db.close()

def save_raw_currency_to_db(db: Session, valutes: dict) -> RawCurrency:
    raw_currency = RawCurrency(
        json_data=valutes
    )
    db.add(raw_currency)
    db.flush()
    logger.info(f"Saved raw currency data with ID: {raw_currency.id}")
    return raw_currency


def save_structured_currencies(db: Session, raw_currency: RawCurrency) -> list:
    """Преобразует сырые данные в структурированные"""
    currency_result = []
    history_mappings = []

    for code, data in raw_currency.json_data.items():
        if not isinstance(data, dict):
            continue

        value_rub = Decimal(str(data.get('Value', 0))) / Decimal(str(data.get('Nominal', 1)))

        # Обновляем или создаем справочник валют
        currency = db.query(Currency).filter(Currency.code == code).first()
        if not currency:
            currency = Currency(
                code=code,
                name=data.get('Name', code),
                value_in_rubles=value_rub
            )
            db.add(currency)
        else:
            currency.value_in_rubles = value_rub

        currency_result.append(currency)

        # Добавляем запись в историю
        history_mappings.append({
            'code': code,
            'name': data.get('Name', code),
            'value_in_rubles': str(value_rub),
            'timestamp': datetime.now(timezone.utc)
        })

    # Массовая вставка истории
    if history_mappings:
        db.bulk_insert_mappings(CurrencyHistory, history_mappings)

    db.flush()
    logger.info(f"Saved {len(currency_result)} currencies and {len(history_mappings)} history records")
    return currency_result


def check_currency_anomalies(db: Session, currencies: list) -> Dict:
    """Проверяет аномалии в курсах валют"""
    anomalies = {}

    for currency in currencies:
        anomaly = compare_currency_rates(db, currency)
        if anomaly:
            anomalies[currency.code] = anomaly
            db.add(anomaly)

    if anomalies:
        db.flush()

    return anomalies

def compare_currency_rates(db: Session, current_rate: Currency) -> Optional[dict]:
    """Аналог compare_weather для валют"""
    code = current_rate.code
    value_rub = current_rate.value_in_rubles
    radius_hours = 24



    # 2. + История этой валюты за 24ч
    history_rates = db.query(CurrencyHistory).filter(
        CurrencyHistory.code == code,
        CurrencyHistory.timestamp > datetime.now(timezone.utc) - timedelta(hours=radius_hours)
    ).all()


    rates = [hr.value_in_rubles for hr in history_rates
             if hr.value_in_rubles is not None]

    if not rates:
        return None

    avg_rate = sum(rates) / len(rates)

    threshold = 0.02  # 2%
    is_anomaly = abs(value_rub - Decimal(str(avg_rate))) > Decimal(str(threshold)) * Decimal(str(abs(avg_rate)))
    if not is_anomaly:
        return None
    logger.warning(f"Найден резкий скачок или падение в валюте {code}")
    anomaly = CurrencySharpChange(
        change_percents= ((value_rub - Decimal(str(avg_rate))) / Decimal(str(avg_rate))) * 100,
        currency = current_rate,
        value_in_rubles = value_rub,
        previous_value = avg_rate,
    )

    return anomaly

