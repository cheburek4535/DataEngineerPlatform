package kafka.integration.models;

public class AQ {
    public record AQRaw(
            int loc_id,
            double pm25,
            double pm10,
            double no2,
            double o3,
            double so2,
            double co
    ) {}
}
