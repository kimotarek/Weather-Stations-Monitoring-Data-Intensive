package org.example.Controller;

import org.example.Storage.KVImpl;
import org.example.models.Pair;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/kv")
public class KVController {
 private final KVImpl kvImpl;
    public KVController(KVImpl kvImpl){
        this.kvImpl = kvImpl;
        kvImpl.start();
    }

    @RequestMapping("/get")
    public String get(@RequestParam String key) {
        return kvImpl.getRecord(key);
    }
    @PostMapping("/put")
    public void put(@RequestParam String key, @RequestParam String value) {
        kvImpl.putRecord(key, value);
    }
    @PostMapping("/putBatch")
    public void putInBatch(@RequestBody List<Pair<String,String>> data) {
        for(Pair<String,String> pair : data){
            kvImpl.putRecord(pair.getFirst(), pair.getSecond());
        }
    }

}
