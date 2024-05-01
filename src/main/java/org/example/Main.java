package org.example;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import org.example.FileManager.FileManagerService;
import org.example.Storage.KVService;
import org.example.models.Header;
import org.example.models.Record;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;

import static java.net.HttpURLConnection.HTTP_OK;

@SpringBootApplication
@RestController
@RequestMapping("/bitCask")
public class Main {
    @Autowired
    KVService kvService;
    public static void main(String[] args) throws IOException {
        SpringApplication.run(Main.class, args);
    }
    @PostMapping("/set/{key}/{value}")
    public ResponseEntity<?> putRecord(@PathVariable String key, @PathVariable String value) {
        kvService.putRecord(key, value);
        return ResponseEntity.status(HTTP_OK).body("Record added successfully");
    }

    @GetMapping("/record/{key}")
    public String getRecord(@PathVariable String key) {
        return kvService.getRecord(key);
    }
}