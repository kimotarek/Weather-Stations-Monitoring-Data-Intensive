package org.example.models;
import lombok.NoArgsConstructor;
import org.apache.avro.Schema;
@NoArgsConstructor
public class StationMessage {
    int stationID;
    int sNo;
    String batteryStatus;
    long statusTimestamp;
    Weather weather;

    public StationMessage(int stationID, int sNo, String batteryStatus, long statusTimestamp, Weather weather) {
        this.stationID = stationID;
        this.sNo = sNo;
        this.batteryStatus = batteryStatus;
        this.statusTimestamp = statusTimestamp;
        this.weather = weather;
    }

    public static Schema getMessageSchema() {
        Schema.Parser parser = new Schema.Parser();
        String MessageSchema = 
                "{\"type\":\"record\",\"name\":\"StationMessage\",\"fields\":[" +
                "{\"name\":\"Station_ID\",\"type\":\"int\"}," +
                "{\"name\":\"S_No\",\"type\":\"int\"}," +
                "{\"name\":\"Battery_Status\",\"type\":\"string\"}," +
                "{\"name\":\"Status_Timestamp\",\"type\":\"long\"}," +
                "{\"name\":\"Weather\",\"type\":{\"type\":\"record\",\"name\":\"Weather\",\"fields\":[" +
                "{\"name\":\"Humidity\",\"type\":\"int\"}," +
                "{\"name\":\"Temprature\",\"type\":\"int\"}," +
                "{\"name\":\"Wind_Speed\",\"type\":\"int\"}" +
                "]}}]}";
        return parser.parse(MessageSchema);
    }


    public int getStationID(){
        return stationID;
    }

    public int getsNo(){
        return sNo;
    }
    
    public String getBatteryStatus(){
        return batteryStatus;
    }
    
    public long getStatusTimestamp(){
        return statusTimestamp;
    }

    public Weather getWeather(){
        return weather;
    }

}