package org.example.FileManager;

import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import org.example.models.Header;
import org.example.models.Meta;
import org.example.models.Record;

import java.io.*;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Map;

public class FileManagerService {

    private File file;
   public FileManagerService() {
       createDir();
       getActiveFile();
   }
   public File getFile() {
       return file;
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

    private void getActiveFile(){
       File[] files = getSortedFiles();
         if(files.length==0){
              createFile();
         }
         else{
              file=files[files.length-1];
         }
   }

    public void createDir(){
        try {
            File directory = new File(FileConfig.DB_DIRECTORY);
            if (!directory.exists()) {
                if (directory.mkdirs()) {
                    System.out.println("Directory created successfully.");
                } else {
                    System.out.println("Failed to create directory.");
                }
            } else {
                System.out.println("Directory already exists.");
            }
        }
        catch (Exception e){
            System.out.println("Error occurred while creating directory.");
        }
    }
    public void createFile(){
        try {
             file = new File(FileConfig.DB_DIRECTORY + "/" + FileConfig.FILE_PREFIX + System.currentTimeMillis());
            if (file.createNewFile()) {
                System.out.println("File created: " + file.getName());
            } else {
                System.out.println("File already exists.");
            }
        }
        catch (Exception e){
            System.out.println("Error occurred while creating file.");
        }
    }
    public void createHintFile(Long fileId){
        try {
            File file = new File(FileConfig.DB_DIRECTORY + "/" + FileConfig.HINT_FILE+fileId);
            if (file.createNewFile()) {
                System.out.println("File created: " + file.getName());
            } else {
                System.out.println("File already exists.");
            }
        }
        catch (Exception e){
            System.out.println("Error occurred while creating hint file.");
        }
    }
    public File createCompactFile(){
        File file = new File(FileConfig.DB_DIRECTORY + "/" + FileConfig.FILE_PREFIX + System.currentTimeMillis());
        try {
            if (file.createNewFile()) {
                System.out.println("File created: " + file.getName());
            } else {
                System.out.println("File already exists.");
            }
        }
        catch (Exception e){
            System.out.println("Error occurred while creating file.");
        }
        return file;
    }

    public void checkFileSize(){
        if(file.length() >= FileConfig.FILE_MEMORY_THRESHOLD){
            createFile();
        }
    }

    public int writeToFile(Record record,File file){
       if(file==this.file)
          checkFileSize();
        int offset= (int) file.length();
        try {
            ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
            byteArrayOutputStream.write(serializeRecord(record));

            try(FileOutputStream fileOutputStream = new FileOutputStream(file, true)){
                fileOutputStream.write(byteArrayOutputStream.toByteArray());
            }
            return offset;
        }
        catch (Exception e){
            System.out.println("Error occurred while writing to file.");
        }
        return offset;
    }
    public String checkForHintFile(){
        File directory = new File(FileConfig.DB_DIRECTORY);
        File[] files = directory.listFiles();
        if(files==null)
            return null;
        for(File file:files){
            if(file.getName().contains(FileConfig.HINT_FILE)){
                return file.getName();
            }
        }
        return null;
    }
    public void writeToHintFile(Map<String , Meta> keyDir,Long fileId){
       // delete the old hint file
       String hintFileName=checkForHintFile();
        if(hintFileName!=null){
            File file = new File(FileConfig.DB_DIRECTORY + "/" + hintFileName);
            file.delete();
        }
       // create a new hint file
       createHintFile(fileId);
       String filePath= FileConfig.DB_DIRECTORY + "/" + FileConfig.HINT_FILE+fileId;
        try (ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(filePath, false))) {
            // write keyDir size to the hint file
            oos.writeInt(keyDir.size());
            oos.writeObject(keyDir);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public byte[] serializeHeader(Record record){
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        Header header = record.getHeader();
        try {
            byteArrayOutputStream.write(Longs.toByteArray(header.getTimeStamp()));
            byteArrayOutputStream.write(Ints.toByteArray(header.getKeySize()));
            byteArrayOutputStream.write(Ints.toByteArray(header.getValueSize()));
        }
        catch (Exception e){
            System.out.println("Error occurred while serializing header.");
        }
        return byteArrayOutputStream.toByteArray();
    }
    public byte[] serializeRecord(Record record){
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        try {
            byteArrayOutputStream.write(serializeHeader(record));
            byteArrayOutputStream.write(record.getKey().getBytes());
            byteArrayOutputStream.write(record.getValue().getBytes());
        }
        catch (Exception e){
            System.out.println("Error occurred while serializing record.");
        }
        return byteArrayOutputStream.toByteArray();
    }

    public byte[] readRandom(Long fileId,int pos,int size){
        byte[] buffer = new byte[size];
        try {
            String filePath= FileConfig.DB_DIRECTORY + "/" + FileConfig.FILE_PREFIX+fileId;
            // Open the file in read-only mode
            RandomAccessFile randomAccessFile = new RandomAccessFile(filePath, "r");
            // Move to the specified position in the file
            randomAccessFile.seek(pos);
            int bytesRead = randomAccessFile.read(buffer);
            // Close the file
            randomAccessFile.close();
        } catch (IOException e) {
            System.out.println("An error occurred: " + e.getMessage());
            e.printStackTrace();
        }
        return buffer;

    }

}
