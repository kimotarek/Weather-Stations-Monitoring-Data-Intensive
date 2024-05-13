package org.example.WeatherKafkaUsingJSONOrProtobufService;

import io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.example.WeatherData.MessageCreator;
import proto.WeatherStatusMessageOuterClass;

import java.util.Properties;

public class KafkaProducer {

    Properties properties = new Properties();
    MessageCreator messageCreator=new MessageCreator();

    private void setUp(){
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaProtobufSerializer.class.getName());

        properties.put("schema.registry.url", "http://localhost:8081");
    }


    public void sendMessageProto(){

        this.setUp();
        Producer<String, WeatherStatusMessageOuterClass.WeatherStatusMessage> producer = new org.apache.kafka.clients.producer.KafkaProducer<>(properties);
        try {

            String topic = "WeatherStatusMessages";
            String key = "WeatherKey";

            WeatherStatusMessageOuterClass.WeatherStatusMessage weatherRecord = messageCreator.CreateWeatherStatusMessageProto();
            if(weatherRecord == null) return;

            ProducerRecord<String,  WeatherStatusMessageOuterClass.WeatherStatusMessage> producerRecord = new ProducerRecord<>(topic, key, weatherRecord);


            System.out.println(weatherRecord.toString());
            producer.send(producerRecord).get();
            producer.flush();
            producer.close();


        } catch (Exception e) {
            // Handle interruption exception
            e.printStackTrace();
        }finally {
            producer.flush();
            producer.close();
        }

    }


    public void sendMessageJsonString(){

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new org.apache.kafka.clients.producer.KafkaProducer<>(props);
        try {

            String topic = "WeatherStatusMessages";

            String weatherRecord = messageCreator.CreateWeatherStatusMessageJSON();
            if(weatherRecord == null) return;

            ProducerRecord<String, String> record = new ProducerRecord<>(topic, weatherRecord);

            System.out.println(weatherRecord);
            producer.send(record);
            producer.flush();
            producer.close();

        } catch (Exception e) {
            // Handle interruption exception
            e.printStackTrace();
        }finally {
            producer.flush();
            producer.close();
        }

    }

}
