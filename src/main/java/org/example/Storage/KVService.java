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
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
@Service
public class KVService {
    Map<String, Meta> keyDir;
  FileManagerService fileManagerService;
    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    public KVService() {
        this.keyDir = new HashMap<>();
        this.fileManagerService = new FileManagerService();
    }
    public Map<String, Meta> getKeyDir() {
        return keyDir;
    }
    public void start(){
        rebuild();
        // Schedule the compaction task to run every 5 seconds
        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
        scheduler.scheduleAtFixedRate(this::mergeFiles, 5, 5, TimeUnit.SECONDS);
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
        boolean success= lock.writeLock().tryLock();
        if(!success){
            System.out.println("Failed to acquire lock.");
        }
        try {
            keyDir.put(key, meta);
        }
        finally {
            lock.writeLock().unlock();
        }


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
     lock.readLock().lock();
     Meta meta ;
     try{
         meta = keyDir.get(key);
     } finally {
         lock.readLock().unlock();
     }
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
        File[] files = directory.listFiles((dir, name) -> !name.startsWith("hint_file_"));

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
              while(offset<byteRead){
                    Record record= parseRecord(bytesArray,offset);
                    offset+=record.getRecordSize();
                    mergeMap.put(record.getKey(),new Pair<>(record.getValue(),record.getHeader().getTimeStamp()));
              }
        }
        catch (IOException e){
            System.out.println("Error occurred while reading file.");
        }
    }
    private Record parseRecord(byte[] bytesArray,int offset)
    {   byte[] dest =new byte[FileConfig.TIMESTAMP_LENGTH];
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

        return new Record(new Header(timestamp,keySize,valueSize),key,value);
    }

    public  void mergeFiles() {
        try {
            System.out.println("Compaction started.");
            File[] files = getSortedFiles();
            System.out.println("Files to be merged: " + Arrays.toString(files));
            files = Arrays.copyOfRange(files, 0, files.length - 1);
            System.out.println("Files to be merged: " + Arrays.toString(files));
            Long lastFileId = extractFileId(files[files.length - 1].getName());
            if (files.length < 2) {
                System.out.println("No files to merge.");
                return;
            }
            HashMap<String, Pair<String, Long>> mergeMap = new HashMap<>();
            for (File file : files) {
                getKVFromFile(file, mergeMap);
            }
            File mergedFile = fileManagerService.createCompactFile();
            System.out.println("Merged file created.");
            for (Map.Entry<String, Pair<String, Long>> entry : mergeMap.entrySet()) {
                Pair<String, Long> pair = entry.getValue();
                putMergedRecord(entry.getKey(), pair.getFirst(), pair.getSecond(), mergedFile);
            }
            System.out.println("Records written to merged file.");
            System.out.println("Compaction completed.");
            // update the keyDir
            lock.writeLock().lock();
            try {
                for (Map.Entry<String, Pair<String, Long>> entry : mergeMap.entrySet()) {
                    String key = entry.getKey();
                    Pair<String, Long> pair = entry.getValue();
                    Meta meta = keyDir.get(key);
                    if (keyDir.containsKey(key) && Objects.equals(meta.getTimeStamp(), pair.getSecond())) {
                        meta.setFileId(extractFileId(mergedFile.getName()));
                        keyDir.put(key, meta);
                    }
                }
                System.out.println("KeyDir updated.");
                // delete the old files
                for (File file : files) {
                    System.out.println(file.delete());
                }
            }
            finally {
                lock.writeLock().unlock();
            }


            // rename the merged file
            mergedFile.renameTo(new File(FileConfig.DB_DIRECTORY + "/" + FileConfig.FILE_PREFIX + lastFileId));

            // write to the hint file
            fileManagerService.writeToHintFile(keyDir, lastFileId);
            System.out.println("Hint file updated.");
        }
        catch (Exception e){
            System.out.println("Error occurred while merging files.");
        }


    }
    private Boolean rebuildFromHintFile(){
        String hintFileName= fileManagerService.checkForHintFile();
        if(hintFileName==null){
            return false;
        }
        File hintFile = new File(FileConfig.DB_DIRECTORY + "/" + hintFileName);
        if (!hintFile.exists()) {
            return false;
        }
        try {
            FileInputStream fis = new FileInputStream(hintFile);
            ObjectInputStream ois = new ObjectInputStream(fis);
            int size = ois.readInt();
            keyDir = (HashMap<String, Meta>) ois.readObject();
            // if the db crashed while writing to the hint file
            if(keyDir.size()!=size){
                return false;
            }
            // read records from files that have larger timestamp than the hint file
            File directory = new File(FileConfig.DB_DIRECTORY);
            File[] files = directory.listFiles((dir, name) -> !name.startsWith("hint_file_"));
            for (File file:files){
               if(extractFileId(file.getName())>extractHintFileId(hintFile.getName())){
                   byte[] bytesArray = new byte[(int) file.length()];
                   FileInputStream fis1 = new FileInputStream(file);
                   int byteRead = fis1.read(bytesArray);

                   int offset = 0;
                   while (offset < byteRead) {
                       Record record = parseRecord(bytesArray, offset);
                       Meta meta = new Meta(record.getHeader().getTimeStamp(), record.getRecordSize(), offset, extractFileId(file.getName()));
                       keyDir.put(record.getKey(), meta);
                       offset += record.getRecordSize();
                   }
               }
            }

        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }
        return true;

    }
    private void rebuildFromOriginalFiles(){
        File[] files = getSortedFiles();
        for(File file:files) {
            try {
                byte[] bytesArray = new byte[(int) file.length()];
                FileInputStream fis = new FileInputStream(file);
                int byteRead = fis.read(bytesArray);

                int offset = 0;
                while (offset < byteRead) {
                    Record record = parseRecord(bytesArray, offset);
                    Meta meta = new Meta(record.getHeader().getTimeStamp(), record.getRecordSize(), offset, extractFileId(file.getName()));
                    keyDir.put(record.getKey(), meta);
                    offset += record.getRecordSize();
                }
            } catch (IOException e) {
                System.out.println("Error occurred while reading file.");
            }
        }
    }
    public void rebuild(){
        if(rebuildFromHintFile()){
            System.out.println("Rebuild from hint file successful.");
            return;
        }
        System.out.println("Rebuild from hint file failed.");
        System.out.println("Rebuilding from original files...");
        rebuildFromOriginalFiles();
        System.out.println("Rebuild from original files successful.");
    }
    private Long extractFileId(String fileName) {
        String[] parts = fileName.split("_");
        return Long.parseLong(parts[1]);
    }
    private Long extractHintFileId(String fileName) {
        String[] parts = fileName.split("_");
        return Long.parseLong(parts[2]);
    }
    }

