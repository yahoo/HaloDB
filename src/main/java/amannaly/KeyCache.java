package amannaly;

import com.google.protobuf.ByteString;

public interface KeyCache {

    boolean put(ByteString key, RecordMetaData metaData);

    RecordMetaData get(ByteString key);

    boolean replace(ByteString key, RecordMetaData oldValue, RecordMetaData newValue);

    boolean containsKey(ByteString key);

    void close();

    /********************************
     * FOR TESTING.
     ********************************/

    void printPutLatency();

    void printGetLatency();

    void printMapContents();
}
