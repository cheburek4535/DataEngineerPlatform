package kafka.integration;

import com.fasterxml.jackson.databind.ObjectMapper;
import kafka.integration.models.Weather;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class WeatherStreamApp {

    private static final ObjectMapper mapper = new ObjectMapper();
    private static final Logger log = LoggerFactory.getLogger(WeatherStreamApp.class);

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "weather-consumer-app");

        String kafkaServer = System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServer);

        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");


        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> stream = builder.stream("weather.raw");

        KStream<String, Weather.RawWeather> rawWeatherKStream = stream.mapValues(value -> {
            try {
                log.info("Пойман JSON: {}", value);
                return mapper.readValue(value, Weather.RawWeather.class);
            } catch (Exception e) {
                log.error("Ошибка парсинга JSON", e);
                return null;
            }
        }).filter((key, value) -> value != null);

        rawWeatherKStream.process(() -> new BatchingProcessor("weather"));

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
        streams.start();
        System.out.println("Kafka Streams запущено");

    }
}
