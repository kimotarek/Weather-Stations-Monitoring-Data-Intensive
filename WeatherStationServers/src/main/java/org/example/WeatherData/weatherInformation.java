package org.example.WeatherData;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
public class weatherInformation {

    int humidity;
    int temperature;
    int wind_speed;

    weatherInformation(){
        this.setHumidity(35);
        this.setTemperature(100);
        this.setWind_speed(13);
    }

}
