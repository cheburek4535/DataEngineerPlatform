# Берем официальный образ Airflow той версии, что указана у вас в yaml
FROM apache/airflow:2.9.3

# Копируем список зависимостей
COPY requirements.txt .

# Устанавливаем их
RUN pip install --no-cache-dir -r requirements.txt
