package org.example.ChannelSystem;


import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import org.apache.http.HttpEntity;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import java.io.IOException;
import java.util.ArrayList;

import com.google.gson.Gson;
import com.google.gson.JsonObject;

public class Service {

    public static void main(String[] args) {
        String apiUrl = "https://api.open-meteo.com/v1/forecast?latitude=27&longitude=30&minutely_15=temperature_2m,relative_humidity_2m,wind_speed_10m&temperature_unit=fahrenheit&timeformat=unixtime&timezone=Africa%2FCairo&forecast_days=1";

        try (CloseableHttpClient httpClient = HttpClients.createDefault()) {
            HttpGet httpGet = new HttpGet(apiUrl);

            try (CloseableHttpResponse response = httpClient.execute(httpGet)) {

                WeatherDataResponse weatherData=new WeatherDataResponse();

                int statusCode = response.getStatusLine().getStatusCode();
                System.out.println("Status Code: " + statusCode);

                String responseBody = EntityUtils.toString(response.getEntity());
                JsonObject jsonResponse = new Gson().fromJson(responseBody, JsonObject.class);
                JsonObject minutely15Object = jsonResponse.getAsJsonObject().getAsJsonObject("minutely_15");
                weatherData.setTime(minutely15Object.getAsJsonArray("time"));
                weatherData.setTemperature_2m(minutely15Object.getAsJsonArray("temperature_2m"));
                weatherData.setRelative_humidity_2m(minutely15Object.getAsJsonArray("relative_humidity_2m"));
                weatherData.setWind_speed_10m(minutely15Object.getAsJsonArray("wind_speed_10m"));
                System.out.println(weatherData.toString());




            } catch (ClientProtocolException e) {
                throw new RuntimeException(e);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
