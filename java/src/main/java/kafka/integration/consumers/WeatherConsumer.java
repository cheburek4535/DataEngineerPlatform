package kafka.integration.consumers;

import com.fasterxml.jackson.databind.ObjectMapper;
import kafka.integration.models.Weather;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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


import static kafka.integration.db.DBManager.*;

public class WeatherConsumer {
    private final static ObjectMapper mapper = new ObjectMapper();
    private final static HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();
    private final static Logger log = LoggerFactory.getLogger(WeatherConsumer.class);

    public static void processBatch(List<Weather.RawWeather> batch) {
        try {
            List<Map<String, Object>> goBatch = new ArrayList<>();

            List<Weather.RawWeather> saved = saveWeather(batch);
            if (saved == null || saved.isEmpty()) {
                log.error("Погодный батч не сохранился, поэтому прерываем обработку");
                return ;
            }
            for (Weather.RawWeather value : saved) {

                Instant timestamp = value.timestamp();

                String collectedAt = (timestamp != null ? timestamp : Instant.now())
                        .truncatedTo(ChronoUnit.SECONDS)
                        .toString();
                Map<String, Object> goItem = Map.of(
                        "locId", value.locationId(),
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
                boolean goBatchSuccess = checkAnomaliesGo(goBatch);
                if (!goBatchSuccess) {
                    log.error("Anomaly check failed");
                }
            }
        } catch (Exception e) {
            log.error("Ошибка обработки батча погоды", e);
        }
    }
    private static boolean checkAnomaliesGo(List<Map<String, Object>> batch) {
        String jsonBody = null;
        try {
            jsonBody = mapper.writeValueAsString(batch);
        } catch (Exception e) {
            log.error("Не удалось превратить goBatch в json", e);
            return false;
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
            log.error("Не удалось отправить батч в Go", e);
            return false;
        }
        if (response.statusCode() != 200) {
            log.error("Go прислал плохой статус: {}", response.statusCode());
            return false;
        }
        Weather.GoResponse result = null;
        try {
            result = mapper.readValue(response.body(), Weather.GoResponse.class);
        } catch (Exception e) {
            log.error("Ошибка десериализации ответа Go", e);
            return false;
        }
        List<Weather.AnomaliesResponse> anomalies = result.result();
        if (!anomalies.isEmpty()) {
            boolean saved = saveAnomalies(anomalies);
            if (!saved) {
                log.warn("Батч Go не был сохранен");
                return false;
            }
        } else {
            log.info("Go не нашел никаких аномалий в погодном батче");
        }
        return true;
    }

}
