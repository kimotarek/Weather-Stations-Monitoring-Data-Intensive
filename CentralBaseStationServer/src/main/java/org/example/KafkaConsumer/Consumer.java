package org.example.KafkaConsumer;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.example.BitCaskConnections.BitCaskConnector;
import org.example.Parquet.ParquetController;
import org.example.models.Pair;
import org.example.models.StationMessage;
import org.example.models.Weather;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class Consumer {
    private final BitCaskConnector bitCaskConnector = new BitCaskConnector();
    private final ParquetController parquetController = new ParquetController();
    Properties properties = new Properties();

    private void setUp(){
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

    }

    void receiveMessages(){
        this.setUp();
        org.apache.kafka.clients.consumer.Consumer<String, String> consumer = new org.apache.kafka.clients.consumer.KafkaConsumer<>(properties);
        consumer.subscribe(Collections.singletonList("WeatherStatusMessages"));

        try {

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                records.forEach(record -> {
                    JsonObject jsonObject = JsonParser.parseString(record.value()).getAsJsonObject();
                    System.out.println(record.value());
//                    Pair<String,String> kv = getKV(jsonObject);
//                    bitCaskConnector.putInDB(kv.getFirst(),kv.getSecond());
//                    JsonObject jsonObject1 = JsonParser.parseString(record.value()).getAsJsonObject();
//                    StationMessage stationMessage = getStationMsg(jsonObject1);
//                    parquetController.AddToParquet(stationMessage);


                });
            }
        } catch (Exception e) {
            // Handle interruption exception
            e.printStackTrace();
        }
    }
    private Pair<String,String> getKV(JsonObject jsonObject){
        String key = jsonObject.get("station_id").getAsString();
        jsonObject.remove("station_id");
        String value = jsonObject.toString();
        return new Pair<>(key,value);
    }
    private StationMessage getStationMsg(JsonObject jsonObject){

        int stationId = jsonObject.get("station_id").getAsInt();
        int s_no = jsonObject.get("s_no").getAsInt();
        long timestamp = jsonObject.get("status_timestamp").getAsLong();
        String batteryStatus = jsonObject.get("battery_status").getAsString();
        JsonObject weather = jsonObject.get("weatherInfo").getAsJsonObject();
        Weather weatherInfo = new Weather(weather.get("temperature").getAsInt(),weather.get("humidity").getAsInt(),weather.get("wind_speed").getAsInt());
        return new StationMessage(stationId,s_no,batteryStatus,timestamp,weatherInfo);
    }

    public static void main(String[] args) {

        Consumer consumer=new Consumer();
        consumer.receiveMessages();

    }
}
