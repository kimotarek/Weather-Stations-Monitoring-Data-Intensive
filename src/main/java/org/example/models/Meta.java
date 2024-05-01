package org.example.models;

import java.io.File;

public class Meta {
    private Long timeStamp;
    private int recordSize;
    private int recordPos;
    File file;

    public Meta(Long timeStamp, int recordSize, int recordPos, File file) {
        this.timeStamp = timeStamp;
        this.recordSize = recordSize;
        this.recordPos = recordPos;
        this.file = file;
    }

    public Long getTimeStamp() {
        return timeStamp;
    }

    public void setTimeStamp(Long timeStamp) {
        this.timeStamp = timeStamp;
    }

    public int getRecordSize() {
        return recordSize;
    }

    public void setRecordSize(int recordSize) {
        this.recordSize = recordSize;
    }

    public int getRecordPos() {
        return recordPos;
    }

    public void setRecordPos(int recordPos) {
        this.recordPos = recordPos;
    }

    public File getFile() {
        return file;
    }

    public void setFile(File file) {
        this.file = file;
    }
}