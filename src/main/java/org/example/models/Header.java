package org.example.models;

public class Header {
    private Long timeStamp;
    private int keySize;
    private int valueSize;

    public Header( Long timeStamp ,int keySize, int valueSize) {
        this.timeStamp = timeStamp;
        this.keySize = keySize;
        this.valueSize = valueSize;
    }

    public Long getTimeStamp() {
        return timeStamp;
    }

    public void setTimeStamp(Long timeStamp) {
        this.timeStamp = timeStamp;
    }

    public int getKeySize() {
        return keySize;
    }

    public void setKeySize(int keySize) {
        this.keySize = keySize;
    }

    public int getValueSize() {
        return valueSize;
    }

    public void setValueSize(int valueSize) {
        this.valueSize = valueSize;
    }
}
