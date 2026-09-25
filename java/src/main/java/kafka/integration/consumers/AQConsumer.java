package kafka.integration.consumers;

import kafka.integration.models.AQ;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class AQConsumer {
    private final static Logger log = LoggerFactory.getLogger(AQConsumer.class);
    public static void processAQ(List<AQ.AQRaw> aq) {
        int locId = aq.loc_id();
        if (locId <=0) {
            log.error("Поле locId пустое в AQ, скип");
            return;
        }
    }
}
