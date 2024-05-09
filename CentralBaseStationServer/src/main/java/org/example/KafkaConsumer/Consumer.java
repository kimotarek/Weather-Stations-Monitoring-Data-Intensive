package org.example.KafkaConsumer;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.example.BitCaskConnections.BitCaskConnector;
import org.example.Parquet.ParquetController;
import org.example.models.Pair;
import org.example.models.StationMessage;
import org.example.models.Weather;
import proto.WeatherStatusMessageOuterClass;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class Consumer {
    private final BitCaskConnector bitCaskConnector = new BitCaskConnector();
    private final ParquetController parquetController = new ParquetController();
    Properties properties = new Properties();

    private void setUp(){
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "Weather-group");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaProtobufDeserializer.class.getName());
        properties.put("schema.registry.url", "http://localhost:8081");
    }
    void receiveMessages(){
        this.setUp();
        org.apache.kafka.clients.consumer.KafkaConsumer<String, WeatherStatusMessageOuterClass.WeatherStatusMessage> consumer = new org.apache.kafka.clients.consumer.KafkaConsumer<>(properties);

        try (consumer) {
            consumer.subscribe(Collections.singletonList("WeatherStatusMessages"));

            while (true) {
                ConsumerRecords<String, WeatherStatusMessageOuterClass.WeatherStatusMessage> records = consumer.poll(Duration.ofMillis(100));
                records.forEach(record -> {
                    try {

                        Pair<String,String> pair = getKV(JsonFormat.printer().print(record.value()));
                        // put in bitCask
//                         bitCaskConnector.putInDB(pair.getFirst(),pair.getSecond());
                         // put in parquet
                        StationMessage stationMessage = getStationMsg(JsonFormat.printer().print(record.value()));
                        parquetController.AddToParquet(stationMessage);
                    } catch (InvalidProtocolBufferException e) {
                        throw new RuntimeException(e);
                    }


                });
            }


        } catch (Exception e) {
            // Handle interruption exception
            e.printStackTrace();
        }
    }
    private Pair<String,String> getKV(String msg){
        JsonObject jsonObject = JsonParser.parseString(msg).getAsJsonObject();
        String key = jsonObject.get("stationId").getAsString();
        jsonObject.remove("stationId");
        String value = jsonObject.toString();
        return new Pair<>(key,value);
    }
    private StationMessage getStationMsg(String msg){

        JsonObject jsonObject = JsonParser.parseString(msg).getAsJsonObject();
        int stationId = jsonObject.get("stationId").getAsInt();
        int s_no = jsonObject.get("sNo").getAsInt();
        long timestamp = jsonObject.get("statusTimestamp").getAsLong();
        String batteryStatus = jsonObject.get("batteryStatus").getAsString();
        JsonObject weather = jsonObject.get("weatherInfo").getAsJsonObject();
        Weather weatherInfo = new Weather(weather.get("temperature").getAsInt(),weather.get("humidity").getAsInt(),weather.get("windSpeed").getAsInt());
        return new StationMessage(stationId,s_no,batteryStatus,timestamp,weatherInfo);
    }

    public static void main(String[] args) {

        Consumer consumer=new Consumer();
        consumer.receiveMessages();

    }
}
