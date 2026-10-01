package kafka.integration.consumers;

import kafka.integration.models.AQ;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

public class AQConsumer {
    private final static Logger log = LoggerFactory.getLogger(AQConsumer.class);
    public static void processAQ(List<Map<String, Object>> batch) {
        for (Map<String, Object> aq : batch) {
            Integer RawlocId = (Integer) aq.get("location_id");
            int locId = RawlocId != null ? RawlocId : 0;
            if (locId <= 0) {
                continue;
            }
            log.info("processing AQ at loc {}", locId);

        }
    }
    private static Map<String, Object> transformAQ(Map<String, Object> aq) {
        List<Map<String, Object>> measurements = (List<Map<String, Object>>) aq.getOrDefault("measurements", List.of(Map.of()));

    }
}
