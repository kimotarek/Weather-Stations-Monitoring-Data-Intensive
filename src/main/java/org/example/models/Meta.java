package org.example.models;

import java.io.File;
import java.io.Serializable;

public class Meta implements Serializable {
    private Long timeStamp;
    private int recordSize;
    private int recordPos;
    Long fileId;

    public Meta(Long timeStamp, int recordSize, int recordPos, Long fileId) {
        this.timeStamp = timeStamp;
        this.recordSize = recordSize;
        this.recordPos = recordPos;
        this.fileId = fileId;
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

    public Long getFileId() {
        return fileId;
    }

    public void setFileId(Long fileId) {
        this.fileId = fileId;
    }
}