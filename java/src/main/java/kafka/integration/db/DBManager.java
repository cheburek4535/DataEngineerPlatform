package kafka.integration.db;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import kafka.integration.models.AQ;
import kafka.integration.models.Currency;
import kafka.integration.models.Weather;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.sql.*;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

@Repository
public class DBManager {
    private static final Logger log = LoggerFactory.getLogger(DBManager.class);

    private final ObjectMapper mapper;
    private final JdbcTemplate jdbcTemplate;

    public DBManager(JdbcTemplate jdbcTemplate, ObjectMapper objectMapper) {
        this.jdbcTemplate = jdbcTemplate;
        this.mapper = objectMapper;
    }

    private String toJsonOrEmpty(Object data) {
        if (data == null) return "{}";
        try {
            return mapper.writeValueAsString(data);
        } catch (Exception e) {
            log.error("Convert object to json string error return empty JSON", e);
            return "{}";
        }
    }
    public List<Weather.RawWeather> saveWeather(List<Weather.RawWeather> weather) {
        log.info("Сохранение погодного бачта в БД");
        String sql = "insert into weather (location_id, timestamp, temperature, pressure, humidity, wind_speed) values (?, ?, ?, ?, ?, ?) ON CONFLICT (location_id, timestamp) DO NOTHING";

        var validBatch = weather.stream().filter(w -> w.location_id() > 0).toList();
        if (validBatch.isEmpty()) {return new ArrayList<>();}

        jdbcTemplate.batchUpdate(sql, validBatch, validBatch.size(),(ps, data) -> {
            ps.setInt(1, data.location_id());
            ps.setTimestamp(2, java.sql.Timestamp.from(java.time.Instant.ofEpochSecond(data.timestamp())));
            ps.setDouble(3, data.temp());
            ps.setDouble(4, data.pressure());
            ps.setDouble(5, data.humidity());
            ps.setDouble(6, data.wind_speed());
        });
        log.info("Saved weather batch with {} records", validBatch.size());
        return validBatch;

    }
    public boolean saveAnomalies(List<Weather.AnomaliesResponse> anomalies) {
        log.info("Сохранение батча аномалтий а БД");
        String sql = """
insert into anomalies (location_id, anomaly_temperature, anomaly_pressure, anomaly_humidity, anomaly_wind_speed, additional_data)
values (?, ?, ?, ?, ?, ?::jsonb)
on conflict (location_id) do update set 
                              anomaly_temperature = EXCLUDED.anomaly_temperature,
                              anomaly_pressure = EXCLUDED.anomaly_pressure,
                              anomaly_humidity = EXCLUDED.anomaly_humidity,
                              anomaly_wind_speed = EXCLUDED.anomaly_wind_speed,
                              additional_data = EXCLUDED.additional_data,
                             found_at = NOW()
""";
            var validBatch = anomalies.stream()
                    .filter(a -> a.anomalies_to_save() != null && a.loc_id() > 0)
                    .toList();
            if (validBatch.isEmpty()) return false;
            jdbcTemplate.batchUpdate(sql, validBatch, validBatch.size(), (ps, anomaly) -> {
                var anomaliesToSave = anomaly.anomalies_to_save();
                var additional = toJsonOrEmpty(anomaly.anomalies_data());
                ps.setInt(1, anomaly.loc_id());
                ps.setObject(2, anomaliesToSave.anomaly_temperature());
                ps.setObject(3, anomaliesToSave.anomaly_pressure());
                ps.setObject(4, anomaliesToSave.anomaly_humidity());
                ps.setObject(5, anomaliesToSave.anomaly_wind_speed());
                ps.setObject(6, additional);
            });
            return true;

    }
    public void saveAQ(List<AQ.AQStructured> batch) {
        log.info("Save AQ batch in DB");
        String sql = "insert into air_quality (location_id, pm25, pm10, no2, o3, so2, co, collected_at) values (?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT (location_id, collected_at) DO NOTHING";

        var validBatch = batch.stream().filter(aq -> aq.loc_id() > 0).toList();
        jdbcTemplate.batchUpdate(sql, validBatch, validBatch.size(), (ps, aq) -> {
            ps.setInt(1, aq.loc_id());
            ps.setObject(2, aq.pm25());
            ps.setObject(3, aq.pm10());
            ps.setObject(4, aq.no2());
            ps.setObject(5, aq.o3());
            ps.setObject(6, aq.so2());
            ps.setObject(7, aq.co());
            ps.setTimestamp(8, java.sql.Timestamp.from(aq.collected_at()));
        });
    }

    public List<Currency.CurrencyEntity> saveCurrencies(List<Currency.CurrencyStructured> batch) {
        List<Currency.CurrencyEntity> saved = new ArrayList<>();
        String sql = """
                insert into currencies (name, code, value_in_rubles) values (?, ?, ?)
                on conflict (code) do update set value_in_rubles = EXCLUDED.value_in_rubles
                returning id, code, name, value_in_rubles
                """;
        try (Connection conn = DriverManager.getConnection(URL, USER, PASS)) {
            conn.setAutoCommit(false);
            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                for (Currency.CurrencyStructured currency : batch) {
                    ps.setString(1, currency.name());
                    ps.setString(2, currency.code());
                    ps.setBigDecimal(3, currency.valueInRubles());
                    try (ResultSet rs = ps.executeQuery()) {
                        if (rs.next()) {
                            saved.add(new Currency.CurrencyEntity(
                                    rs.getInt("id"),
                                    rs.getString("code"),
                                    rs.getString("name"),
                                    rs.getBigDecimal("value_in_rubles")
                            ));
                        }
                    }
                }
                conn.commit();
                return saved;
            } catch (SQLException e) {
                conn.rollback();
                throw e;
            }
        } catch (Exception e) {
            log.error("Currency batch save error", e);
            throw new RuntimeException("Database currency error: " + e.getMessage(), e);
        }
    }

    public boolean saveCurrenciesHistory(List<Currency.CurrencyStructured> batch) {
        String sql = "insert into currency_history (name, code, value_in_rubles) values (?, ?, ?)";

        try (Connection conn = DriverManager.getConnection(URL, USER, PASS);
             PreparedStatement ps = conn.prepareStatement(sql)) {
            int successful = 0;
            for (Currency.CurrencyStructured currency : batch) {
                ps.setString(1, currency.name());
                ps.setString(2, currency.code());
                ps.setBigDecimal(3, currency.valueInRubles());
                ps.addBatch();
                successful++;
            }
            if (successful > 0) {
                ps.executeBatch();
                log.info("Saved currency history batch in DB with {} records", successful);
                return true;
            }
        } catch (Exception e) {
            log.error("Currency history batch save error", e);
            throw new RuntimeException("Database currency history error: " + e.getMessage(), e);
        }
        return false;
    }

    public List<Currency.CurrencyStructured> getCurrencyHistory(String code, Instant since) {
        List<Currency.CurrencyStructured> result = new ArrayList<>();
        String sql = """
                select code, name, value_in_rubles from currency_history
                where code = ? and timestamp > ?
                """;
        try (Connection conn = DriverManager.getConnection(URL, USER, PASS);
        PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, code);
            ps.setTimestamp(2, Timestamp.from(since));

            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    result.add(new Currency.CurrencyStructured(
                            rs.getString("code"),
                            rs.getString("name"),
                            rs.getBigDecimal("value_in_rubles")
                    ));
                }
            }
            return result;
        } catch (Exception e) {
            log.error("Currency history data get error", e);
            throw new RuntimeException("Database currency history get error: " + e.getMessage(), e);
        }
    }
    public void saveCurrencySharpChanges(List<Currency.SharpChange> batch) {
        String sql = "insert into currency_sharp_changes (change_percents, value_in_rubles, previous_value, currency_id) values (?, ?, ?, ?)";
        try (Connection conn = DriverManager.getConnection(URL, USER, PASS);
        PreparedStatement ps = conn.prepareStatement(sql)) {
            int successful = 0;
            for (var sc : batch) {
                ps.setBigDecimal(1, sc.changePercents());
                ps.setBigDecimal(2, sc.valueInRubles());
                ps.setBigDecimal(3, sc.previousValue());
                ps.setInt(4, sc.currencyId());

                ps.addBatch();
                successful++;
            }
            if (successful > 0) {
                ps.executeBatch();
                log.info("Successfully saved currency sharp changes batch in DB");
            }
        } catch (Exception e) {
            log.error("Currency sharp changes saved error", e);
            throw new RuntimeException("Database sharp changes saved error: " + e.getMessage(), e);
        }
    }

}
