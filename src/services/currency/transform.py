from datetime import datetime, timezone
from typing import Optional
from sqlalchemy.orm import Session
from services.db.models import RawCurrency, Currency, CurrencyHistory
from services.currency.extract import collect_raw_currencies, get_currency
from decimal import Decimal


def save_structured_currencies(db: Session, raw_currency: RawCurrency) -> list[Currency]:
    try:
        currency_result = []  # Currency справочник
        history_mappings = []  # СЛОВАРИ для CurrencyHistory!

        for code, data in raw_currency.json_data.items():
            value_rub = Decimal(str(data['Value'])) / Decimal(str(data['Nominal']))

            # 1. Currency справочник (обновляем)
            currency = db.query(Currency).filter(Currency.code == code).first()
            if not currency:
                currency = Currency(code=code, name=data['Name'], value_in_rubles=value_rub)
                db.add(currency)
            else:
                currency.value_in_rubles = value_rub

            currency_result.append(currency)

            # 2. НОВАЯ запись в историю (словарь!)
            history_mappings.append({
                'code': code,
                'name': data['Name'],
                'value_in_rubles': str(value_rub),  # Decimal → str!
                'timestamp': datetime.now(timezone.utc)  # Или server_default
            })

        # Bulk insert истории
        if history_mappings:
            db.bulk_insert_mappings(CurrencyHistory, history_mappings)

        db.commit()
        return currency_result

    except Exception:
        db.rollback()
        raise



def collect_currencies(db: Session) -> Optional[list[Currency]]:
    currencies = get_currency()
    if currencies:
        raw_currencies = collect_raw_currencies(db, currencies)
        structured_currency = save_structured_currencies(db, raw_currencies)
        return structured_currency if structured_currency else None
    return None