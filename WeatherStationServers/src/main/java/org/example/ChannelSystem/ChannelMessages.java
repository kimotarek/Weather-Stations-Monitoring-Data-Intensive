package org.example.ChannelSystem;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ChannelMessages {
    Properties properties = new Properties();

    private void setUp(){
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
    }


//    void sendMessage(){
//
//
//        //try and catch
//        this.setUp();
//        try {
//            Producer<String, String> producer = new org.apache.kafka.clients.producer.KafkaProducer<>(properties);
//            String topic = "WeatherRealData";
//
//            if(currentMessage == null) return;
//
//            ProducerRecord<String, String> record = new ProducerRecord<>(topic, currentMessage.toString());
//            System.out.println(currentMessage.toString());
//            producer.send(record);
//            producer.close();
//
//        } catch (Exception e) {
//            // Handle interruption exception
//            e.printStackTrace();
//        }
//
//    }
}
