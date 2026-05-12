from typing import Optional, Dict
from sqlalchemy.orm import Session
from sqlalchemy import and_
from services.db.models import CurrencyHistory, CurrencySharpChange, Currency, RawCurrency
from services.currency.transform import collect_currencies, save_structured_currencies
from logger import logger
from datetime import datetime, timedelta, timezone
from decimal import Decimal


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

    threshold = 0.03  # 3%
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
    db.add(anomaly)
    db.commit()
    db.refresh(anomaly)
    return anomaly


def check_currency_anomalies(db: Session) -> Optional[Dict[str, CurrencySharpChange]]:
    currencies = collect_currencies(db)
    if currencies:
        result = {}
        for currency in currencies:
            sharp_change = compare_currency_rates(db, currency)
            if sharp_change:
                result[currency.code] = sharp_change
        return result
    return None



