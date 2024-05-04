package org.example.Storage;

import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import org.example.FileManager.FileConfig;
import org.example.FileManager.FileManagerService;
import org.example.models.Header;
import org.example.models.Meta;
import org.example.models.Pair;
import org.example.models.Record;
import org.springframework.stereotype.Service;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.*;

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
       int recordPos= fileManagerService.writeToFile(record,fileManagerService.getFile());
       String fileName= fileManagerService.getFile().getName();
        Meta meta = new Meta(System.currentTimeMillis(), recordSize,recordPos, extractFileId(fileName));
        keyDir.put(key, meta);

    }
    public void putMergedRecord(String key,String value,Long timeStamp,File file){
        Header header = new Header(timeStamp, key.length(), value.length());
        Record record = new Record(header, key, value);
        int recordPos= fileManagerService.writeToFile(record,file);
    }

    public String getRecord(String key){
     if(!keyDir.containsKey(key)){
         return "Key not found";
     }
     Meta meta = keyDir.get(key);
     byte[] record= fileManagerService.readRandom(meta.getFileId(), meta.getRecordPos(), meta.getRecordSize());
     byte[] dest =new byte[FileConfig.VALUE_SIZE_LENGTH];
     System.arraycopy(record, FileConfig.VALUE_SIZE_OFFSET, dest, 0, FileConfig.VALUE_SIZE_LENGTH);
     int valueSize= Ints.fromByteArray(dest);
     dest =new byte[valueSize];
     System.arraycopy(record, FileConfig.KEY_OFFSET+key.length(), dest, 0, valueSize);
     return new String(dest);
    }

    public File[] getSortedFiles(){
        File directory = new File(FileConfig.DB_DIRECTORY);
        File[] files = directory.listFiles();

        // Sort files by timestamp
        Arrays.sort(files, new Comparator<>() {
            @Override
            public int compare(File file1, File file2) {
                long timestamp1 = extractTimestamp(file1.getName());
                long timestamp2 = extractTimestamp(file2.getName());
                return Long.compare(timestamp1, timestamp2);
            }

            private long extractTimestamp(String fileName) {
                String[] parts = fileName.split("_");
                return Long.parseLong(parts[1]);
            }
        });
        return files;
    }
    public void getKVFromFile(File file,HashMap<String, Pair<String,Long>> mergeMap){

        try {
           byte[] bytesArray = new byte[(int) file.length()];
           FileInputStream fis = new FileInputStream(file);
           int byteRead= fis.read(bytesArray);

              int offset=0;
              byte[] dest;
              while(offset<byteRead){
                  dest =new byte[FileConfig.TIMESTAMP_LENGTH];
                    System.arraycopy(bytesArray, offset, dest, 0, FileConfig.TIMESTAMP_LENGTH);
                    Long timestamp= Longs.fromByteArray(dest);
                  offset+=FileConfig.TIMESTAMP_LENGTH;
                  dest =new byte[FileConfig.KEY_SIZE_LENGTH];
                  System.arraycopy(bytesArray, offset, dest, 0, FileConfig.KEY_SIZE_LENGTH);
                  int keySize= Ints.fromByteArray(dest);
                  offset+=FileConfig.KEY_SIZE_LENGTH;
                    dest =new byte[FileConfig.VALUE_SIZE_LENGTH];
                    System.arraycopy(bytesArray, offset, dest, 0, FileConfig.VALUE_SIZE_LENGTH);
                    int valueSize= Ints.fromByteArray(dest);
                    offset+=FileConfig.VALUE_SIZE_LENGTH;
                    dest =new byte[keySize];
                    System.arraycopy(bytesArray, offset, dest, 0, keySize);
                    String key= new String(dest);
                    offset+=keySize;
                    dest =new byte[valueSize];
                    System.arraycopy(bytesArray, offset, dest, 0, valueSize);
                    String value= new String(dest);
                    offset+=valueSize;
                    mergeMap.put(key,new Pair<>(value,timestamp));
              }
        }
        catch (IOException e){
            System.out.println("Error occurred while reading file.");
        }
    }

    public void mergeFiles() {
        File[] files = getSortedFiles();
        files = Arrays.copyOfRange(files, 0, files.length - 1);
        if (files.length < 2) {
            return;
        }
        HashMap<String, Pair<String,Long>> mergeMap = new HashMap<>();
        for (File file : files) {
            getKVFromFile(file, mergeMap);
        }
        File mergedFile = fileManagerService.createCompactFile();
        for (Map.Entry<String, Pair<String,Long>> entry : mergeMap.entrySet()) {
            Pair<String,Long> pair= entry.getValue();
            putMergedRecord(entry.getKey(), pair.getFirst(),pair.getSecond(), mergedFile);
        }
        // update the keyDir
        for(Map.Entry<String,Pair<String,Long>> entry: mergeMap.entrySet()){
            String key= entry.getKey();
            Pair<String,Long> pair= entry.getValue();
            Meta meta= keyDir.get(key);
            if(keyDir.containsKey(key) && Objects.equals(meta.getTimeStamp(), pair.getSecond())){
                meta.setFileId(extractFileId(mergedFile.getName()));
                keyDir.put(key,meta);
            }
        }

        // delete the old files
        for (File file : files) {
            file.delete();
        }
        // update the hint file
        fileManagerService.writeToHintFile(keyDir);


    }
    private Boolean rebuildFromHintFile(){
        File hintFile = new File(FileConfig.DB_DIRECTORY + "/" + FileConfig.HINT_FILE);
        if (!hintFile.exists()) {
            return false;
        }
        try {
            FileInputStream fis = new FileInputStream(hintFile);
            ObjectInputStream ois = new ObjectInputStream(fis);
            keyDir = (HashMap<String, Meta>) ois.readObject();
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }
        return true;

    }
    private void rebuildFromOriginalFiles(){
        // To be implemented
    }
    public void rebuild(){
        if(rebuildFromHintFile()){
            return;
        }
        rebuildFromOriginalFiles();
    }
    private Long extractFileId(String fileName) {
        String[] parts = fileName.split("_");
        return Long.parseLong(parts[1]);
    }
    }

