package kafka.integration.consumers;

import kafka.integration.models.Weather;
import org.apache.kafka.shaded.com.google.protobuf.Any;

import java.util.List;
import java.util.Map;

import static kafka.integration.db.DBManager.saveWeather;

public class WeatherConsumer {
    public static void processBatch(List<Weather.RawWeather> batch) {
        try {
            for (Weather.RawWeather value : batch) {
                if !(value.location_id()) {
                    continue;
                }
                saveWeather(value);

                Map<String, Any> goItem = {
                        "id"
                }
            }
        }
    }
    private static void checkAnomaliesGo() {}

}
