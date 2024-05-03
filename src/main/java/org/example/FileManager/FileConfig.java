package org.example.FileManager;

public class FileConfig {
    public static final int TIMESTAMP_OFFSET = 0;

    public static final int TIMESTAMP_LENGTH = 8;

    public static final int KEY_SIZE_OFFSET = 8;

    public static final int KEY_SIZE_LENGTH = 4;

    public static final int VALUE_SIZE_OFFSET = 12;

    public static final int VALUE_SIZE_LENGTH = 4;

    public static final int KEY_OFFSET = 16;

    public static final String FILE_PREFIX = "file_";

    public static final String DB_DIRECTORY = "KV-db";
    public static final String HINT_FILE="hint_file";

    public static final long FILE_MEMORY_THRESHOLD = 8000L;

    public FileConfig(){}
}
