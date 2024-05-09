package org.example.Storage;

import com.google.common.primitives.Ints;
import org.example.FileManager.FileConfig;
import org.example.FileManager.FileManager;
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
public class KVImpl {
    Map<String, Meta> keyDir;
  FileManager fileManager;
  StorageUtil storageUtil;
    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    public KVImpl() {
        this.keyDir = new HashMap<>();
        this.fileManager = new FileManager();
        this.storageUtil = new StorageUtil();
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
        int recordPos= fileManager.writeToFile(record, fileManager.getFile());
       String fileName= fileManager.getFile().getName();
        Meta meta = new Meta(System.currentTimeMillis(), recordSize,recordPos,
                storageUtil.extractFileId(fileName));
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
        int recordPos= fileManager.writeToFile(record,file);
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
     byte[] record= fileManager.readRandom(meta.getFileId(), meta.getRecordPos(), meta.getRecordSize());
     byte[] dest =new byte[FileConfig.VALUE_SIZE_LENGTH];
     System.arraycopy(record, FileConfig.VALUE_SIZE_OFFSET, dest, 0, FileConfig.VALUE_SIZE_LENGTH);
     int valueSize= Ints.fromByteArray(dest);
     dest =new byte[valueSize];
     System.arraycopy(record, FileConfig.KEY_OFFSET+key.length(), dest, 0, valueSize);
     return new String(dest);
    }



    public  void mergeFiles() {
        try {
            System.out.println("Compaction started.");
            File[] files = storageUtil.getSortedFiles();
            System.out.println("Files to be merged: " + Arrays.toString(files));
            files = Arrays.copyOfRange(files, 0, files.length - 1);
            System.out.println("Files to be merged: " + Arrays.toString(files));
            Long lastFileId = storageUtil.extractFileId(files[files.length - 1].getName());
            if (files.length < 2) {
                System.out.println("No files to merge.");
                return;
            }
            HashMap<String, Pair<String, Long>> mergeMap = new HashMap<>();
            for (File file : files) {
                storageUtil.getKVFromFile(file, mergeMap);
            }
            File mergedFile = fileManager.createCompactFile();
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
                        meta.setFileId(storageUtil.extractFileId(mergedFile.getName()));
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
            fileManager.writeToHintFile(keyDir, lastFileId);
            System.out.println("Hint file updated.");
        }
        catch (Exception e){
            System.out.println("Error occurred while merging files.");
        }


    }
    private Boolean rebuildFromHintFile(){
        String hintFileName= fileManager.checkForHintFile();
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
               if(storageUtil.extractFileId(file.getName())> storageUtil.extractHintFileId(hintFile.getName())){
                   byte[] bytesArray = new byte[(int) file.length()];
                   FileInputStream fis1 = new FileInputStream(file);
                   int byteRead = fis1.read(bytesArray);

                   int offset = 0;
                   while (offset < byteRead) {
                       Record record = storageUtil.parseRecord(bytesArray, offset);
                       Meta meta = new Meta(record.getHeader().getTimeStamp(), record.getRecordSize(), offset, storageUtil.extractFileId(file.getName()));
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
        File[] files = storageUtil.getSortedFiles();
        for(File file:files) {
            try {
                byte[] bytesArray = new byte[(int) file.length()];
                FileInputStream fis = new FileInputStream(file);
                int byteRead = fis.read(bytesArray);

                int offset = 0;
                while (offset < byteRead) {
                    Record record = storageUtil.parseRecord(bytesArray, offset);
                    Meta meta = new Meta(record.getHeader().getTimeStamp(), record.getRecordSize(), offset, storageUtil.extractFileId(file.getName()));
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

    }

