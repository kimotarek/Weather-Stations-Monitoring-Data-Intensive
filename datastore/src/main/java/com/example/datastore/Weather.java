package com.example.datastore;
import org.apache.avro.Schema;

public class Weather {
    int Humidity;
    int Temprature;
    int Wind_Speed;

    public Weather(int Humidity, int Temprature, int Wind_Speed) {
        this.Humidity = Humidity;
        this.Temprature = Temprature;
        this.Wind_Speed = Wind_Speed;
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
        return Humidity;
    }

    public int getTemprature(){
        return Temprature;
    }

    public int getWind_Speed(){
        return Wind_Speed;
    }

}
