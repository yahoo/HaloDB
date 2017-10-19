package amannaly;

public interface KeyCache {

    boolean put(byte[] key, RecordMetaData metaData);

    RecordMetaData get(byte[] key);

    boolean replace(byte[] key, RecordMetaData oldValue, RecordMetaData newValue);

    boolean containsKey(byte[] key);

    void close();

    /********************************
     * FOR TESTING.
     ********************************/

    void printPutLatency();

    void printGetLatency();

    void printMapContents();
}
