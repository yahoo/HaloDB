package amannaly;

import amannaly.cache.CacheSerializer;

import java.nio.ByteBuffer;

/**
 * @author Arjun Mannaly
 */
public class RecordMetaDataSerializer implements CacheSerializer<RecordMetaDataForCache> {

    public void serialize(RecordMetaDataForCache recordMetaData, ByteBuffer byteBuffer) {
        recordMetaData.serialize(byteBuffer);
        byteBuffer.flip();
    }

    public RecordMetaDataForCache deserialize(ByteBuffer byteBuffer) {
        return RecordMetaDataForCache.deserialize(byteBuffer);
    }

    public int serializedSize(RecordMetaDataForCache recordMetaData) {
        return RecordMetaDataForCache.SERIALIZED_SIZE;
    }
}
