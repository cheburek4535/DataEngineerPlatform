package kafka.integration;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import kafka.integration.consumers.AQConsumer;
import kafka.integration.consumers.WeatherConsumer;
import kafka.integration.models.Weather;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Properties;

public class Main {

    private static final ObjectMapper mapper = new ObjectMapper();
    private static final Logger log = LoggerFactory.getLogger(Main.class);
    private static final StreamsBuilder builder = new StreamsBuilder();

    static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "weather-consumer-app");

        String kafkaServer = System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServer);

        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 4);


        ProcessStream("weather.raw");
        ProcessStream("air_quality.raw");

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
        streams.start();
        log.info("Kafka Streams started");

    }
    private static boolean ProcessStream(String topic) {

        KStream<String, String> stream = builder.stream(topic);
        switch (topic) {
            case "weather.raw":
                KStream<String, Weather.RawWeather> rawWeatherKStream = stream.mapValues(value -> {
                    try {
                        log.info("catch Weather JSON: {}", value);
                        return mapper.readValue(value, Weather.RawWeather.class);
                    } catch (Exception e) {
                        log.error("Weather JSON parse error", e);
                        return null;
                    }
                }).filter((key, value) -> value != null);

                rawWeatherKStream.process(() -> new BatchingProcessor<>(250, WeatherConsumer::processBatch));
                return true;

            case "air_quality.raw":
                KStream<String, Map<String, Object>> rawAQKStream = stream.mapValues(value -> {
                    try {
                        log.info("catch AQ JSON: {}", value);
                        return mapper.readValue(value, new TypeReference<Map<String, Object>>() {});
                    } catch (Exception e) {
                        log.error("AQ JSON parse error", e);
                        return null;
                    }
                }).filter((key, value) -> value != null);

                rawAQKStream.process(() -> new BatchingProcessor<>(50, AQConsumer::processAQ));
                return true;

            default:
                log.warn("Incorrect type of kafka topic");
                return false;
        }

    }
}
