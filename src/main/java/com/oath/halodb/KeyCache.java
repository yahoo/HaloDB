package com.oath.halodb;

public interface KeyCache {

    boolean put(byte[] key, RecordMetaDataForCache metaData);

    RecordMetaDataForCache get(byte[] key);

    boolean remove(byte[] key);

    boolean replace(byte[] key, RecordMetaDataForCache oldValue, RecordMetaDataForCache newValue);

    boolean containsKey(byte[] key);

    void close();

    long size();

    String stats();

    /********************************
     * FOR TESTING.
     ********************************/

    void printPutLatency();

    void printGetLatency();
}
