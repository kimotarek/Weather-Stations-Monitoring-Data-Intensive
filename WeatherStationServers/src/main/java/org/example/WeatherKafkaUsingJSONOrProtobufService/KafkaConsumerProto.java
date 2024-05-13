package org.example.WeatherKafkaUsingJSONOrProtobufService;

import io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.example.WeatherData.MessageCreator;
import proto.WeatherStatusMessageOuterClass;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class KafkaConsumerProto {

    Properties properties = new Properties();
    MessageCreator messageCreator=new MessageCreator();

    private void setUp(){
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "Weather-group");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer .class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaProtobufDeserializer.class.getName());
        properties.put("schema.registry.url", "http://localhost:8081");
    }



    void receiveMessages(){
        this.setUp();
        org.apache.kafka.clients.consumer.KafkaConsumer<String,WeatherStatusMessageOuterClass.WeatherStatusMessage> consumer = new org.apache.kafka.clients.consumer.KafkaConsumer<>(properties);

        try (consumer) {
            consumer.subscribe(Collections.singletonList("WeatherStatusMessages"));

            while (true) {
                ConsumerRecords<String, WeatherStatusMessageOuterClass.WeatherStatusMessage> records = consumer.poll(Duration.ofMillis(100));
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

        KafkaConsumerProto consumer=new KafkaConsumerProto();
        consumer.receiveMessages();

    }
}
