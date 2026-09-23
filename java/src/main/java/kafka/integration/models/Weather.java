package kafka.integration.models;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.Instant;
import java.util.List;

public class Weather {
    public record RawWeather(
            @JsonProperty("location_id")
            int locationId,
            double temp,
            double wind_speed,
            double humidity,
            double pressure,
            double latitude,
            double longitude,
            Instant timestamp
    ) {}
    public record GoResponse(
            int processed,
            List<AnomaliesResponse> result
    ) {}
    public record AnomaliesResponse(
            @JsonProperty("loc_id")
            int locId,
            AnomaliesToSave anomalies_to_save,
          AnomaliesData anomalies_data
    ) {}
    public record AnomaliesToSave(
            Double anomaly_temperature,
            Double anomaly_pressure,
            Double anomaly_humidity,
            Double anomaly_wind_speed
    ) {}
    public record AnomaliesData(
            Metric temperature,
            Metric pressure,
            Metric humidity,
            Metric wind_speed
    ) {}
    public record Metric(
            Double value,
            Double avg
    ) {}

}
