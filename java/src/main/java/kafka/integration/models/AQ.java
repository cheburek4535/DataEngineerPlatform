package kafka.integration.models;

public class AQ {
    public record AQStructured(
            int loc_id,
            double pm25,
            double pm10,
            double no2,
            double o3,
            double so2,
            double co
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