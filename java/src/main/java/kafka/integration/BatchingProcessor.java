package kafka.integration;

import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

public class BatchingProcessor<T> implements Processor<String, T, Void, Void> {
    private static final Logger log = LoggerFactory.getLogger(BatchingProcessor.class);

    private final int batchSize;
    private final Consumer<List<T>> batchConsumer;
    private final List<T> batch = new ArrayList<>();
    private ProcessorContext<Void, Void> context;

    public BatchingProcessor(int batchSize, Consumer<List<T>> batchConsumer) {
        this.batchSize = batchSize;
        this.batchConsumer = batchConsumer;
    }

    @Override
    public void init(ProcessorContext<Void, Void> context) {
        this.context = context;
        context.schedule(Duration.ofSeconds(10), PunctuationType.WALL_CLOCK_TIME, timestamp -> flushBatch());
    }

    @Override
    public void process(Record<String, T> record) {
        batch.add(record.value());
        if (batch.size() >= batchSize) {
            flushBatch();
        }
    }

    public void flushBatch() {
        if (batch.isEmpty()) {
            return;
        }
        try {
            // Вызываем переданную логику для конкретного типа
            batchConsumer.accept(new ArrayList<>(batch));
        } catch (Exception e) {
            log.error("Ошибка при обработке батча: ", e);
        } finally {
            batch.clear();
        }
    }

    @Override
    public void close() {
        flushBatch();
    }
}