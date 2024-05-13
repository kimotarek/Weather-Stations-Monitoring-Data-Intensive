package org.example.WeatherKafkaUsingJSONOrProtobufService;

import com.google.gson.Gson;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.example.WeatherData.WeatherStatusMessage;

import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

public class KafkaProcessor {

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "humidity-checker");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        AtomicInteger H = new AtomicInteger();
        StreamsBuilder builder = new StreamsBuilder();
        builder.stream("WeatherStatusMessages", Consumed.with(Serdes.String(), Serdes.String()))
                .filter((key, value) -> {
                        Gson gson = new Gson();
                        WeatherStatusMessage message = gson.fromJson(value, WeatherStatusMessage.class);
                        int humidityValue = message.getWeatherInfo().getHumidity();
                        try {
                            H.set(humidityValue);
                            return humidityValue > 70;
                        } catch (NumberFormatException e) {
                            // Handle invalid humidity values
                            return false;
                        }
                })
                .map((key, value) -> new KeyValue<>("key", "The humidity is "+H+" > 70%; it may rain.")) // Send "hi" message to the new topic
                .to("Humidity", Produced.with(org.apache.kafka.common.serialization.Serdes.String(), org.apache.kafka.common.serialization.Serdes.String()));

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
