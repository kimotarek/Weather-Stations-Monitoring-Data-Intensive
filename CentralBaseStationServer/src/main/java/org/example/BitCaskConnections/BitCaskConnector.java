package org.example.BitCaskConnections;

import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;

public class BitCaskConnector {
    public void putInDB(String key,String value) {
        String url = "http://localhost:8080/kv/put";

        try {
            URL obj = new URL(url);
            HttpURLConnection con = (HttpURLConnection) obj.openConnection();

            // Set the request method to POST
            con.setRequestMethod("POST");

            // Set the request headers
            con.setRequestProperty("Content-Type", "application/x-www-form-urlencoded");

            // Enable output and disable input
            con.setDoOutput(true);
            con.setDoInput(true);

            // Construct the request body with parameters
            String requestBody = "key=" + key + "&value=" + value;

            // Write the request body to the connection's output stream
            try (OutputStream os = con.getOutputStream()) {
                byte[] input = requestBody.getBytes(StandardCharsets.UTF_8);
                os.write(input, 0, input.length);
            }

            // Get the response code
            int responseCode = con.getResponseCode();

            // Print the response code
            System.out.println("Response Code: " + responseCode);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
