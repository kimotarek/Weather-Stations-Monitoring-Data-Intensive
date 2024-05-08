package org.example.Controller;

import org.example.Storage.KVService;
import org.example.dto.MsgDto;
import org.example.models.Pair;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/kv")
public class KVController {
 private final KVService kvService;
    public KVController(KVService kvService){
        this.kvService = kvService;
        kvService.start();
    }

    @RequestMapping("/get")
    public String get(@RequestParam String key) {
        return kvService.getRecord(key);
    }
    @PostMapping("/put")
    public void put(@RequestParam String key, @RequestParam String value) {
        kvService.putRecord(key, value);
    }
    @PostMapping("/putinbatch")
    public void putInBatch(@RequestBody List<Pair<String,String>> data) {
        for(Pair<String,String> pair : data){
            kvService.putRecord(pair.getFirst(), pair.getSecond());
        }
    }

}
