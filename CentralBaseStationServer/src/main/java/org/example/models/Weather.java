package org.example.models;

import lombok.NoArgsConstructor;
import org.apache.avro.Schema;
@NoArgsConstructor
public class Weather {
    int humidity;
    int temperature;
    int windSpeed;

    public Weather(int Humidity, int temperature, int windSpeed) {
        this.humidity = Humidity;
        this.temperature = temperature;
        this.windSpeed = windSpeed;
    }


    public static Schema getWeatherSchema() {
        Schema.Parser parser = new Schema.Parser();
        String WeatherSchema = 
                "{\"type\":\"record\",\"name\":\"Weather\",\"fields\":[" +
                "{\"name\":\"Humidity\",\"type\":\"int\"}," +
                "{\"name\":\"Temprature\",\"type\":\"int\"}," +
                "{\"name\":\"Wind_Speed\",\"type\":\"int\"}" +
                "]}";
        return parser.parse(WeatherSchema);
    }

    public int getHumidity(){
        return humidity;
    }


    public int getTemperature(){
        return temperature;
    }

    public int getWindSpeed(){
        return windSpeed;
    }

}
