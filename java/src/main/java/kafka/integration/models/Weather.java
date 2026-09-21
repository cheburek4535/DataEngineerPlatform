package kafka.integration.models;

import java.time.Instant;

public class Weather {
    public record RawWeather(
            int location_id,
            double temp,
            double wind_speed,
            double humidity,
            double pressure,
            double latitude,
            double longitude,
            Instant timestamp
    ) {}

}
