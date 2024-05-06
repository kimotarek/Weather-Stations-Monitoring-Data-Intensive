package org.example.WeatherData;

import lombok.Data;
import lombok.ToString;

@Data
@ToString
public class WeatherStatusMessage {

    long station_id;
    long s_no;
    String battery_status;
    long status_timestamp;
    weatherInformation weatherInfo;

}
