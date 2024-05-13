package org.example.WeatherKafkaUsingJSONOrProtobufService;

import io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializer;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.example.WeatherData.MessageCreator;
import proto.WeatherStatusMessageOuterClass;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class KafkaConsumerJSON {

    Properties properties = new Properties();

    private void setUp(){
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

    }



    void receiveMessages(){
        this.setUp();
        Consumer<String, String> consumer = new org.apache.kafka.clients.consumer.KafkaConsumer<>(properties);
        consumer.subscribe(Collections.singletonList("WeatherStatusMessages"));

        try {

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            records.forEach(record -> {
                System.out.println("Received message: " + record.value());
            });
        }
        } catch (Exception e) {
            // Handle interruption exception
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {

        KafkaConsumerJSON consumer=new KafkaConsumerJSON();
        consumer.receiveMessages();

    }
}
