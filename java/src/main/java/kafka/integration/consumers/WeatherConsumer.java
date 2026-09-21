package kafka.integration.consumers;

import com.fasterxml.jackson.databind.ObjectMapper;
import kafka.integration.models.Weather;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static kafka.integration.db.DBManager.saveWeather;

public class WeatherConsumer {
    private final static ObjectMapper mapper = new ObjectMapper();
    private final static HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();

    public static void processBatch(List<Weather.RawWeather> batch) {
        try {
            List<Map<String, Object>> goBatch = new ArrayList<>();
            for (Weather.RawWeather value : batch) {
                if (value.location_id() <= 0) {
                    continue;
                }
                saveWeather(value);

                Instant timestamp = value.timestamp();

                String collectedAt = (timestamp != null ? timestamp : Instant.now())
                        .truncatedTo(ChronoUnit.SECONDS)
                        .toString();
                Map<String, Object> goItem = Map.of(
                        "loc_id", value.location_id(),
                        "lat",value.latitude(),
                        "lon", value.longitude(),
                        "temperature", value.temp(),
                        "pressure", value.pressure(),
                        "humidity", value.humidity(),
                        "wind_speed", value.wind_speed(),
                        "collected_at", collectedAt
                );
                goBatch.add(goItem);
            }
            if (!goBatch.isEmpty()) {
                checkAnomaliesGo(goBatch);
            }
        } catch (Exception e) {
            System.out.printf("Ошибка обработки батча погоды: %s", e);
        }
    }
    private static void checkAnomaliesGo(List<Map<String, Object>> batch) {
        String jsonBody = null;
        try {
            jsonBody = mapper.writeValueAsString(batch);
        } catch (Exception e) {
            System.out.println("Не удалось превратить goBatch в json");
            return;
        }
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("http://golang:8000/weather/batch"))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(jsonBody))
                .build();

        HttpResponse<String> response = null;
        try {
            response = client.send(request, HttpResponse.BodyHandlers.ofString());
        } catch (Exception e) {
            System.out.println("Не удалось отправить батч в Go");
            return;
        }
        if (response.statusCode() != 200) {
            System.out.printf("Go прислал плохой статус: %d", response.statusCode());
            return;
        }
        Weather.GoResponse result = null;
        try {
            result = mapper.readValue(response.body(), Weather.GoResponse.class);
        } catch (Exception e) {
            System.out.println("Ошибка десериализации ответа Go");
            return;
        }
        List<Weather.AnomaliesResponse> anomalies = result.result();
        if (!anomalies.isEmpty()) {
            for (Weather.AnomaliesResponse anomaly : anomalies) {
                Weather.AnomaliesToSave anomaliesToSave = anomaly.anomalies_to_save();
                int locId = anomaly.loc_id();
                Weather.AnomaliesData anomaliesData = anomaly.anomalies_data();
                if (anomaliesToSave != null && locId >= 0) {
                    saveAnomaly();
                }
            }
        }

    }

}
