CREATE TABLE IF NOT EXISTS weather_analytics.weather (
    id          UInt64,
    lat         Float64,
    lon         Float64,
    timestamp   DateTime64(3, 'UTC'),
    temperature Float64,
    pressure    Float64,
    humidity    Float64,
    wind_speed  Float64,
    collected_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = MergeTree()
ORDER BY (lat, lon, timestamp)
PARTITION BY toYYYYMM(timestamp)
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS weather_analytics.anomalies (
    id                  UInt64,
    lat                 Float64,
    lon                 Float64,
    location_id         UInt64,
    anomaly_temperature Nullable(Float64),
    anomaly_humidity    Nullable(Float64),
    anomaly_wind_speed  Nullable(Float64),
    anomaly_pressure    Nullable(Float64),
    additional_data     String,
    found_at            DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = MergeTree()
ORDER BY (lat, lon, found_at)
PARTITION BY toYYYYMM(found_at)
SETTINGS index_granularity = 8192;

-- Справочник валют (маленькая, без партиций)
CREATE TABLE IF NOT EXISTS weather_analytics.currencies (
    id UInt64,
    name LowCardinality(String),
    code LowCardinality(String),
    value_in_rubles Decimal128(16),  -- ClickHouse Decimal
    created_at DateTime64(3, 'UTC') DEFAULT now64(3),
    updated_at Nullable(DateTime64(3, 'UTC'))
)
ENGINE = MergeTree()
ORDER BY (code, created_at)  -- code основной!
SETTINGS index_granularity = 8192;

-- Скачки (аналитика, по времени)
CREATE TABLE IF NOT EXISTS weather_analytics.currency_sharp_changes (
    id UInt64,
    change_percents Float64,
    value_in_rubles Decimal128(16),
    previous_value Nullable(Decimal128(16)),
    currency_code LowCardinality(String),  -- ← code вместо ID! FK нет в CH
    found_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = MergeTree()
ORDER BY (currency_code, found_at)  -- code + время
PARTITION BY toYYYYMM(found_at)
SETTINGS index_granularity = 8192;





--CREATE TABLE IF NOT EXISTS weather_analytics.raw_weather (
--    id          UInt64,
--    lat         Float64,
--    lon         Float64,
--    data_json   String,
--    collected_at DateTime64(3, 'UTC') DEFAULT now64(3)
--)
--ENGINE = MergeTree()
--ORDER BY (lat, lon, collected_at)
--PARTITION BY toYYYYMM(collected_at)
--SETTINGS index_granularity = 8192;
--
--CREATE TABLE IF NOT EXISTS weather_analytics.locations_to_track (
--    id                  UInt64,
--    lat                 Float64,
--    lon                 Float64,
--    check_interval      Int64,
--    last_checked_at     DateTime64(3, 'UTC'),
--    created_at          DateTime64(3, 'UTC') DEFAULT now64(3)
--)
--ENGINE = MergeTree()
--ORDER BY (lat, lon, created_at)
--PARTITION BY toYYYYMM(created_at)
--SETTINGS index_granularity = 8192;

