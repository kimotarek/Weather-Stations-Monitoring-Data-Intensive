package org.example.ChannelSystem;

import com.google.gson.JsonArray;
import lombok.Data;
import lombok.ToString;

@Data
@ToString
public class WeatherDataResponse {

    private JsonArray time;
    private JsonArray temperature_2m;
    private JsonArray relative_humidity_2m;
    private JsonArray wind_speed_10m;

}
