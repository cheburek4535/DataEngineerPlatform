
"""
gRPC клиент для связи с Go сервисом расчета качества жизни
"""
import grpc
import sys
from typing import List, Dict, Optional

# Добавляем путь к сгенерированным proto файлам
sys.path.insert(0, '/opt/airflow/src/generated')

import generated.go.service_pb2 as service_pb2
import generated.go.service_pb2_grpc as service_pb2_grpc
from google.protobuf import symbol_database as _symbol_database
from logger import logger

# Получаем доступ ко всем protobuf классам через symbol_database
_sym_db = _symbol_database.Default()

# Извлекаем нужные классы по их полным именам в пакете "go"
try:
    LocationData = _sym_db.GetSymbol('go.LocationData')
    AirQualityData = _sym_db.GetSymbol('go.AirQualityData')
    WeatherData = _sym_db.GetSymbol('go.WeatherData')
    AnomalyData = _sym_db.GetSymbol('go.AnomalyData')
    BatchRequest = _sym_db.GetSymbol('go.BatchRequest')
    BatchResponse = _sym_db.GetSymbol('go.BatchResponse')
    LifeScoreResponse = _sym_db.GetSymbol('go.LifeScoreResponse')
    logger.info("Successfully loaded all protobuf message types")
except KeyError as e:
    logger.error(f"Failed to load protobuf types: {e}")
    raise


class LifeScoreClient:
    """Клиент для gRPC сервиса LifeScoreService"""

    def __init__(self, host: str = "golang", port: int = 50051):
        """
        Инициализация gRPC клиента

        Args:
            host: хост Go сервиса (в Docker сети - имя контейнера)
            port: порт gRPC сервера
        """
        self.channel = grpc.insecure_channel(
            f"{host}:{port}",
            options=[
                ('grpc.max_send_message_length', 100 * 1024 * 1024),  # 100MB
                ('grpc.max_receive_message_length', 100 * 1024 * 1024),
                ('grpc.keepalive_time_ms', 30000),
                ('grpc.keepalive_timeout_ms', 10000),
            ]
        )
        self.stub = service_pb2_grpc.LifeScoreServiceStub(self.channel)
        logger.info(f"gRPC client connected to {host}:{port}")

    def calculate_batch(self, locations: List[Dict]) -> Optional[List[Dict]]:
        """
        Отправка всех локаций одним запросом

        Args:
            locations: список словарей с данными локаций

        Returns:
            список словарей с результатами или None при ошибке
        """
        if not locations:
            logger.warning("Empty locations list")
            return None

        try:
            # Создаем batch запрос
            batch = BatchRequest()

            for loc in locations:
                # Создаем LocationData для каждой локации
                loc_data = self._create_location_data(loc)
                batch.locations.append(loc_data)

            logger.info(f"Sending {len(batch.locations)} locations to Go service")

            # Вызываем удаленную процедуру
            response = self.stub.CalculateBatch(batch, timeout=30)

            # Конвертируем результат
            result = []
            for score in response.scores:
                result.append({
                    'location_id': score.location_id,
                    'general_score': score.general_score,
                    'air_quality': score.air_quality,
                    'weather_quality': score.weather_quality,
                    'anomalies_danger': score.anomalies_danger,
                })

            logger.info(f"Received {len(result)} scores from Go service")
            return result

        except grpc.RpcError as e:
            logger.error(f"gRPC error: code={e.code()}, details={e.details()}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error: {e}", exc_info=True)
            return None

    def _create_location_data(self, loc: Dict):
        """
        Создает LocationData сообщение из словаря
        """
        loc_data = LocationData()
        loc_data.loc_id = int(loc['loc_id'])

        # Добавляем AirQuality данные
        for aq in loc.get('aq', []):
            aq_msg = AirQualityData()
            if aq.get('pm25') is not None:
                aq_msg.pm25 = float(aq['pm25'])
            if aq.get('pm10') is not None:
                aq_msg.pm10 = float(aq['pm10'])
            if aq.get('no2') is not None:
                aq_msg.no2 = float(aq['no2'])
            if aq.get('o3') is not None:
                aq_msg.o3 = float(aq['o3'])
            if aq.get('so2') is not None:
                aq_msg.so2 = float(aq['so2'])
            if aq.get('co') is not None:
                aq_msg.co = float(aq['co'])
            loc_data.aq.append(aq_msg)

        # Добавляем Weather данные
        for w in loc.get('weather', []):
            w_msg = WeatherData()
            if w.get('temp') is not None:
                w_msg.temp = float(w['temp'])
            if w.get('pres') is not None:
                w_msg.pres = float(w['pres'])
            if w.get('hum') is not None:
                w_msg.hum = float(w['hum'])
            if w.get('wind') is not None:
                w_msg.wind = float(w['wind'])
            loc_data.weather.append(w_msg)

        # Добавляем Anomaly данные
        for a in loc.get('anomalies', []):
            a_msg = AnomalyData()
            if a.get('temp') is not None:
                a_msg.temp = float(a['temp'])
            if a.get('hum') is not None:
                a_msg.hum = float(a['hum'])
            if a.get('pres') is not None:
                a_msg.pres = float(a['pres'])
            if a.get('wind') is not None:
                a_msg.wind = float(a['wind'])
            loc_data.anomalies.append(a_msg)

        return loc_data

    def close(self):
        """Закрываем gRPC канал"""
        if self.channel:
            self.channel.close()
            logger.info("gRPC channel closed")