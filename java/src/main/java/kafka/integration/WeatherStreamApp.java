package kafka.integration;

import com.fasterxml.jackson.databind.ObjectMapper;
import kafka.integration.models.Weather;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;

public class WeatherStreamApp {

    private static final ObjectMapper mapper = new ObjectMapper();

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "weather-aggregator-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());


        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> stream = builder.stream("weather.raw");

        KStream<String, Weather.RawWeather> rawWeatherKStream = stream.mapValues(value -> {
            try {
                return mapper.readValue(value, Weather.RawWeather.class);
            } catch (Exception e) {
                return null;
            }
        }).filter((key, value) -> value != null);

        rawWeatherKStream.process(() -> new BatchingProcessor());

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
        streams.start();
        System.out.println("Kafka Streams запущено");

    }
}
