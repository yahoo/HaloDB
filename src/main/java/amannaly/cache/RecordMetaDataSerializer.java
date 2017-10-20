package amannaly.cache;

import amannaly.RecordMetaDataForCache;

import org.caffinitas.ohc.CacheSerializer;

import java.nio.ByteBuffer;

public class RecordMetaDataSerializer implements CacheSerializer<RecordMetaDataForCache> {

    public void serialize(RecordMetaDataForCache recordMetaData, ByteBuffer byteBuffer) {
        byteBuffer.putInt(recordMetaData.fileId);
        byteBuffer.putLong(recordMetaData.offset);
        byteBuffer.putInt(recordMetaData.recordSize);
        byteBuffer.flip();
    }

    public RecordMetaDataForCache deserialize(ByteBuffer byteBuffer) {
        int fileId = byteBuffer.getInt();
        long offset = byteBuffer.getLong();
        int size = byteBuffer.getInt();

        return new RecordMetaDataForCache(fileId, offset, size);
    }

    public int serializedSize(RecordMetaDataForCache recordMetaData) {
        return 4 + 8 + 4;
    }
}
