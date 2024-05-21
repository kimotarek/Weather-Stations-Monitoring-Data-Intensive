package org.example;

import org.example.WeatherKafkaUsingJSONOrProtobufService.KafkaProducer;

public class Main {
    public static void main(String[] args) {
        String id = System.getenv("ID");
        KafkaProducer kafkaProducer = new KafkaProducer();
        while (true) {
            kafkaProducer.sendMessageJsonString(Integer.parseInt(id));

            try {
                // Sleep for 1 second (1000 milliseconds)
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                // Handle interruption exception
                e.printStackTrace();
                System.out.println("Error main");
            }
        }
    }

}
