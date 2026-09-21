package kafka.integration;

import kafka.integration.consumers.WeatherConsumer;
import kafka.integration.models.Weather;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.internals.InternalTopologyBuilder;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

public class BatchingProcessor implements Processor<String, Weather.RawWeather, Void, Void> {
    private ProcessorContext<Void, Void> context;
    private final List<Weather.RawWeather> batch = new ArrayList<>();
    private final static int BATCH_SIZE = 100;

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
        if (batch.isEmpty()) {return;}
        System.out.println("Отправка погодного бачта на обработку");
        WeatherConsumer.processBatch();
        batch.clear();
    }
    @Override
    public void close() {
        flushBatch();
    }

}
