package kafka.integration;

import kafka.integration.consumers.AQConsumer;
import kafka.integration.consumers.WeatherConsumer;
import kafka.integration.models.Weather;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.internals.InternalTopologyBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;

public class BatchingProcessor<T> implements Processor<String, T, Void, Void> {
    private final Consumer<List<T>> batchConsumer;
    private static final Logger log = LoggerFactory.getLogger(BatchingProcessor.class);
    private ProcessorContext<Void, Void> context;
    private final List<Weather.RawWeather> batch = new ArrayList<>();
    private final static int BATCH_SIZE = 100;

    public BatchingProcessor(Consumer<List<T>> batchConsumer) {
        this.batchConsumer = batchConsumer;
    }

    @Override
    public void init(ProcessorContext<Void, Void> context) {
        this.context = context;

        context.schedule(Duration.ofSeconds(10), PunctuationType.WALL_CLOCK_TIME, timestamp -> {flushBatch();});
    }
    @Override
    public void process(Record<String, Weather.RawWeather> record) {
        batch.add(record.value());
        if (batch.size() >= BATCH_SIZE) {
            flushBatch();
        }
    }
    public void flushBatch() {
        if (batch.isEmpty()) {
            log.error("Батч пустой");
            return;
        }

        try {
            if (Objects.equals(type, "weather")) {
                log.info("Отправка погодного бачта на обработку");
                WeatherConsumer.processBatch(batch);
            } else if (Objects.equals(type, "aq")) {
                log.info("Отправка бачта качества воздуха на обработку");
                AQConsumer.processAQ(batch);
            } else {
                log.error("Неверно указан тип обработчика для бачтей");
                return;
            }

        } catch (Exception e) {
            log.error(e.getMessage());
        } finally {
            batch.clear();
        }
    }
    @Override
    public void close() {
        flushBatch();
    }

}
