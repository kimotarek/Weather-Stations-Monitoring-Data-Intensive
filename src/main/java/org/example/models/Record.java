package org.example.models;

public class Record {
    Header header;
    String key;
    String value;

    public Record(Header header, String key, String value) {
        this.header = header;
        this.key = key;
        this.value = value;
    }
public int getRecordSize(){
        return header.getHeaderSize() + key.length() + value.length();
    }
    public Header getHeader() {
        return header;
    }

    public void setHeader(Header header) {
        this.header = header;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }
}
