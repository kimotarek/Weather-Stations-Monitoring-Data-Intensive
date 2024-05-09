package org.example.Storage;

import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import org.example.FileManager.FileConfig;
import org.example.models.Header;
import org.example.models.Pair;
import org.example.models.Record;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;

public class StorageUtil {


    public Long extractFileId(String fileName) {
        String[] parts = fileName.split("_");
        return Long.parseLong(parts[1]);
    }
    public Long extractHintFileId(String fileName) {
        String[] parts = fileName.split("_");
        return Long.parseLong(parts[2]);
    }
    public Record parseRecord(byte[] bytesArray, int offset)
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

    public void getKVFromFile(File file, HashMap<String, Pair<String,Long>> mergeMap){

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
}
