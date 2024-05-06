package org.example.WeatherKafkaUsingProtobufService;

import io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializer;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.example.WeatherData.MessageCreator;
import proto.MyRecordOuterClass;
import proto.WeatherStatusMessageOuterClass;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class KafkaConsumer {

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

        KafkaConsumer consumer=new KafkaConsumer();
        consumer.receiveMessages();

    }
}
