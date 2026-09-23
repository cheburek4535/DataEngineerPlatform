package kafka.integration.db;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import kafka.integration.models.Weather;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class DBManager {
    private static final String URL = "jdbc:postgresql://weather_db:5432/weather_guard";
    private static final String USER = "weather_pass";
    private static String PASS = "weather_pass";
    private static final ObjectMapper mapper = new ObjectMapper();
    private static final Logger log = LoggerFactory.getLogger(DBManager.class);

    public static List<Weather.RawWeather> saveWeather(List<Weather.RawWeather> weather) {
        String sql = "insert into weather (location_id, timestamp, temperature, pressure, humidity, wind_speed) values (?, ?, ?, ?, ?, ?)";
        try (Connection conn = DriverManager.getConnection(URL, USER, PASS);
             PreparedStatement ps = conn.prepareStatement(sql)) {

            List<Weather.RawWeather> savedWeather = new ArrayList<>();
            int successful = 0;
            for (Weather.RawWeather data : weather) {
                if (data.locationId() <= 0) {continue;}
                ps.setInt(1, data.locationId());
                ps.setObject(2, data.timestamp());
                ps.setDouble(3, data.temp());
                ps.setDouble(4, data.pressure());
                ps.setDouble(5, data.humidity());
                ps.setDouble(6, data.wind_speed());

                ps.addBatch();
                savedWeather.add(data);
                successful++;
            }

            if (successful > 0) {
                ps.executeBatch();
            }
            return savedWeather;

        } catch (SQLException e) {
            log.error("Ошибка при сохранении погоды", e);;
            return null;
        }
    }
    public static boolean saveAnomalies(List<Weather.AnomaliesResponse> anomalies) {
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
        try (Connection conn = DriverManager.getConnection(URL, USER, PASS);
             PreparedStatement ps = conn.prepareStatement(sql)) {

            int successful = 0;
            for (Weather.AnomaliesResponse anomaly : anomalies) {
                Weather.AnomaliesToSave anomaliesToSave = anomaly.anomalies_to_save();
                int locId = anomaly.locId();
                Weather.AnomaliesData anomaliesData = anomaly.anomalies_data();
                if (anomaliesToSave != null && locId >= 0) {
                    String additional = null;
                    try {
                        additional = mapper.writeValueAsString(anomaliesData);
                    } catch (JsonProcessingException e) {
                        log.error("Ошибка при сериализации данных об аномалиях, оставляем null", e);
                    }
                    ps.setInt(1, locId);
                    ps.setObject(2, anomaliesToSave.anomaly_temperature());
                    ps.setObject(3, anomaliesToSave.anomaly_pressure());
                    ps.setObject(4, anomaliesToSave.anomaly_humidity());
                    ps.setObject(5, anomaliesToSave.anomaly_wind_speed());
                    ps.setObject(6, additional);

                    ps.addBatch();
                    successful++;
                }
            }
            if (successful > 0) {
                ps.executeBatch();
            }
            return true;

        } catch (SQLException e) {
           log.error("Ошибка при сохранении аномалий", e);
            return false;
        }
    }
}
