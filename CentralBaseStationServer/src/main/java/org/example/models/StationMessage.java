package org.example.models;
import org.apache.avro.Schema;

public class StationMessage {
    int Station_ID;
    int S_No;
    String Battery_Status;
    long Status_Timestamp;
    Weather weather;

    public StationMessage(int Station_ID, int S_No, String Battery_Status, long Status_Timestamp, Weather weather) {
        this.Station_ID = Station_ID;
        this.S_No = S_No;
        this.Battery_Status = Battery_Status;
        this.Status_Timestamp = Status_Timestamp;
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


    public int getStation_ID(){
        return Station_ID;
    }

    public int getS_No(){
        return S_No;
    }
    
    public String getBattery_Status(){
        return Battery_Status;
    }
    
    public long getStatus_Timestamp(){
        return Status_Timestamp;
    }

    public Weather getWeather(){
        return weather;
    }

}