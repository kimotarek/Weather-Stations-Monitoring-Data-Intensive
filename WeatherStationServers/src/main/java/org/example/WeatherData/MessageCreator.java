package org.example.WeatherData;

import proto.MyRecordOuterClass;
import proto.WeatherStatusMessageOuterClass;

import java.util.Random;

public class MessageCreator {

    private static final Random random = new Random();
    private static long st_message = 1;



    public WeatherStatusMessageOuterClass.weatherInformation createWeatherInfo(){

        return WeatherStatusMessageOuterClass.weatherInformation.newBuilder()
                .setHumidity(15)
                .setTemperature(15)
                .setWindSpeed(15)
                .build();
    }

    public WeatherStatusMessageOuterClass.WeatherStatusMessage CreateWeatherStatusMessage(){


        WeatherStatusMessageOuterClass.WeatherStatusMessage Message =WeatherStatusMessageOuterClass.WeatherStatusMessage.newBuilder()
                .setStationId(1)
                .setSNo(st_message++)
                .setBatteryStatus(generateBatteryStatus())
                .setStatusTimestamp(System.currentTimeMillis() / 1000L)
                .setWeatherInfo(createWeatherInfo())
                .build();

        // To simulate data with query-able nature, we create data and then drop it
        if(shouldDropMessage()) return null;
        return Message;
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
