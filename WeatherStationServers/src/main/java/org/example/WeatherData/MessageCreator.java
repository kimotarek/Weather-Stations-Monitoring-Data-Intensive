package org.example.WeatherData;

import com.google.gson.Gson;
import com.google.gson.JsonElement;

import proto.WeatherStatusMessageOuterClass;

import java.util.Random;

public class MessageCreator {


    private static final Random random = new Random();
    private static long st_message_proto = 1;
    private static long st_message_Json = 1;



    public WeatherStatusMessageOuterClass.weatherInformation createWeatherInfoProto(){

        return WeatherStatusMessageOuterClass.weatherInformation.newBuilder()
                .setHumidity(random.nextInt(100))
                .setTemperature(15)
                .setWindSpeed(15)
                .build();
    }

    public WeatherStatusMessageOuterClass.WeatherStatusMessage CreateWeatherStatusMessageProto(){


        WeatherStatusMessageOuterClass.WeatherStatusMessage Message =WeatherStatusMessageOuterClass.WeatherStatusMessage.newBuilder()
                .setStationId(1)
                .setSNo(st_message_proto++)
                .setBatteryStatus(generateBatteryStatus())
                .setStatusTimestamp(System.currentTimeMillis() / 1000L)
                .setWeatherInfo(createWeatherInfoProto())
                .build();

        // To simulate data with query-able nature, we create data and then drop it
        if(shouldDropMessage()) return null;
        return Message;
    }
    public weatherInformation createWeatherInfoJSOn() {

        weatherInformation info = new weatherInformation();
        info.setWind_speed(15);
        info.setTemperature(15);
        info.setHumidity(random.nextInt(100));

        return info;
    }

    public String CreateWeatherStatusMessageJSON(int stationId){

        WeatherStatusMessage weatherMessage=new WeatherStatusMessage();

        weatherMessage.setStation_id(stationId);
        weatherMessage.setBattery_status(generateBatteryStatus());
        weatherMessage.setS_no(st_message_Json++);
        weatherMessage.setStatus_timestamp(System.currentTimeMillis() / 1000L);
        weatherMessage.setWeatherInfo(createWeatherInfoJSOn());


        Gson gson = new Gson();
        String jsonMessage = gson.toJson(weatherMessage);
        // To simulate data with query-able nature, we create data and then drop it
        if(shouldDropMessage()) return null;
        return jsonMessage;

    }



    private String generateBatteryStatus() {
        int randomValue = random.nextInt(100); // Generate a random integer between 0 and 99
        if (randomValue < 30) {
            return "Low";
        } else if (randomValue < 70) {
            return "Medium";
        } else {
            return "High";
        }
    }

    private boolean shouldDropMessage() {
        return random.nextInt(100) < 10; // 10% chance of dropping the message
    }

}
