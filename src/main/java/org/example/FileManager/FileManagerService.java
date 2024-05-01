package org.example.FileManager;

import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import org.example.models.Header;
import org.example.models.Record;

import java.io.*;

public class FileManagerService {

    private File file;
   public FileManagerService() {
       createDir();
       createFile();
   }
   public File getFile() {
       return file;
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

    public void checkFileSize(){
        if(file.length() >= FileConfig.FILE_MEMORY_THRESHOLD){
            createFile();
        }
    }

    public int writeToFile(Record record){
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

    public byte[] readRandom(File file,int pos,int size){
        byte[] buffer = new byte[size];
        try {
            // Open the file in read-only mode
            RandomAccessFile randomAccessFile = new RandomAccessFile(file, "r");
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
