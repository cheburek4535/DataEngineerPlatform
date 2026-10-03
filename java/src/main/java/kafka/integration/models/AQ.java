package kafka.integration.models;

import java.time.Instant;

public class AQ {
    public record AQStructured(
            int loc_id,
            Double pm25,
            Double pm10,
            Double no2,
            Double o3,
            Double so2,
            Double co,
            Instant collected_at
    ) {}
    public record Measurement(
            String parameter,
            Double value,
            String unit,
            String locationName,
            Long SectorId,
            Long locationId
    ) {}
}