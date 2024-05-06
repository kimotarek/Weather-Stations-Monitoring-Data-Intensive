package org.example;

import org.example.WeatherKafkaUsingProtobufService.KafkaProducer;

public class Main {
    public static void main(String[] args) {
        KafkaProducer kafkaProducer = new KafkaProducer();
        System.out.println("ss");
        System.out.println("aa");

        while (true) {
            kafkaProducer.sendMessage();

            try {
                // Sleep for 1 second (1000 milliseconds)
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                // Handle interruption exception
                e.printStackTrace();
            }
        }
    }

}
