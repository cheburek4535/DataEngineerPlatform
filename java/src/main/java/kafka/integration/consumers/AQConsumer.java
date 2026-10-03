package kafka.integration.consumers;

import com.fasterxml.jackson.databind.ObjectMapper;
import kafka.integration.models.AQ;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Instant;
import java.util.*;

import static kafka.integration.db.DBManager.saveAQ;

public class AQConsumer {
    private final static Logger log = LoggerFactory.getLogger(AQConsumer.class);
    private final static ObjectMapper mapper = new ObjectMapper();

    public static void processAQ(List<Map<String, Object>> batch) {
        List<AQ.AQStructured> measurements = new ArrayList<>();
        for (Map<String, Object> aq : batch) {
            Integer RawlocId = (Integer) aq.get("location_id");
            int locId = RawlocId != null ? RawlocId : 0;
            if (locId <= 0) {
                continue;
            }
            log.info("processing AQ at loc {}", locId);
            AQ.AQStructured transformedAQ = transformAQ(aq);
            if (transformedAQ == null) {
                continue;
            }
            measurements.add(transformedAQ);
        }
        if (!measurements.isEmpty()) {
            saveAQ(measurements);
            log.info("AQ batch process ready");
        }
    }
    private static AQ.AQStructured transformAQ(Map<String, Object> aq) {
        Object locIdRaw = aq.get("location_id");
        if (!(locIdRaw instanceof Number locId) || (int) locId <= 0) {
            return null;
        }
        List<Map<String, Object>> measurements = (List<Map<String, Object>>) aq.getOrDefault("measurements", List.of());

        Map<String, List<Double>> params = new HashMap<>();

        for (Map<String, Object> measure : measurements) {
            if (measure == null || measure.isEmpty()) {
                continue;
            }
            AQ.Measurement m;
            try {
                m = mapper.convertValue(measure, AQ.Measurement.class);
            } catch (Exception e) {
                log.error("Can't parse aq measurements", e);
                continue;
            }
            if (m == null || m.locationId() == null ||  m.locationId() <= 0) {
                continue;
            }

            String parameter = m.parameter();
            Double value = m.value();
            if (value != null && parameter != null) {
                params.computeIfAbsent(parameter, k -> new ArrayList<>()).add(value);
            }
        }
        String timeStr = (String) aq.get("collected_at");
        Instant collectedAt = timeStr != null ? Instant.parse(timeStr) : Instant.now();

        Map<String, Double> result = new HashMap<>();
        result.put("pm25", avgOrNull("pm25", params));
        result.put("pm10", avgOrNull("pm10", params));
        result.put("no2", avgOrNull("no2", params));
        result.put("o3", avgOrNull("o3", params));
        result.put("so2", avgOrNull("so2", params));
        result.put("co", avgOrNull("co", params));

        AQ.AQStructured aqResult = new AQ.AQStructured(
                (int) locId,
                result.get("pm25"),
                result.get("pm10"),
                result.get("no2"),
                result.get("o3"),
                result.get("so2"),
                result.get("co"),
                collectedAt

        );
        return aqResult;

    }
    private static Double avgOrNull(String key, Map<String, List<Double>> params) {
        List<Double> list = params.get(key);
        if(list == null|| list.isEmpty()) {
            return null;
        }
  
        double sum = 0;

        for (Double value : list) {
            if (value != null) {
                sum += value;
            }
        }
        double avg = sum / list.size();
        return BigDecimal.valueOf(avg).setScale(3, RoundingMode.HALF_EVEN).doubleValue();

    }
}
