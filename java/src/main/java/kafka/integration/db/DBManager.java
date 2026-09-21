package kafka.integration.db;

import kafka.integration.models.Weather;

import java.sql.*;

public class DBManager {
    private static final String URL = "jdbc:postgresql://weather_db:5432/weather_guard";
    private static final String USER = "weather_pass";
    private static String PASS = "weather_pass";

    public static boolean saveWeather(Weather.RawWeather data) {
        String sql = "insert into weather (location_id, timestamp, temperature, pressure, humidity, wind_speed) values (?, ?, ?, ?, ?, ?)";
        try (Connection conn = DriverManager.getConnection(URL, USER, PASS);
             PreparedStatement ps = conn.prepareStatement(sql)) {

            ps.setInt(1, data.location_id());
            ps.setObject(2, data.timestamp());
            ps.setDouble(3, data.temp());
            ps.setDouble(4, data.pressure());
            ps.setDouble(5, data.humidity());
            ps.setDouble(6, data.wind_speed());

            ps.executeUpdate();
            return true;

        } catch (SQLException e) {
            e.printStackTrace();
            return false;
        }
    }
    public static boolean saveAnomaly(Weather.RawWeather data) {
        String sql = "";
        try (Connection conn = DriverManager.getConnection(URL, USER, PASS);
             PreparedStatement ps = conn.prepareStatement(sql)) {
            return true;

        } catch (SQLException e) {
            e.printStackTrace();
            return false;
        }
    }
}
