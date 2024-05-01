package org.example.Storage;

import com.google.common.primitives.Ints;
import org.example.FileManager.FileConfig;
import org.example.FileManager.FileManagerService;
import org.example.models.Header;
import org.example.models.Meta;
import org.example.models.Record;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;
@Service
public class KVService {
    Map<String, Meta> keyDir;
  FileManagerService fileManagerService;

    public KVService() {
        this.keyDir = new HashMap<>();
       this.fileManagerService = new FileManagerService();
    }

    public void putRecord(String key, String value) {
        Header header = new Header(System.currentTimeMillis(), key.length(), value.length());
        Record record = new Record(header, key, value);
        int recordSize= 16+
                key.length()+
                value.length();
       int recordPos= fileManagerService.writeToFile(record);
        Meta meta = new Meta(System.currentTimeMillis(), recordSize,recordPos, fileManagerService.getFile());
        keyDir.put(key, meta);

    }

    public String getRecord(String key){
     if(!keyDir.containsKey(key)){
         return "Key not found";
     }
     Meta meta = keyDir.get(key);
     byte[] record= fileManagerService.readRandom(meta.getFile(), meta.getRecordPos(), meta.getRecordSize());
     byte[] dest =new byte[FileConfig.VALUE_SIZE_LENGTH];
     System.arraycopy(record, FileConfig.VALUE_SIZE_OFFSET, dest, 0, FileConfig.VALUE_SIZE_LENGTH);
     int valueSize= Ints.fromByteArray(dest);
     dest =new byte[valueSize];
     System.arraycopy(record, FileConfig.KEY_OFFSET+key.length(), dest, 0, valueSize);
     return new String(dest);
    }
    }

