package amannaly.cache;

import amannaly.RecordMetaData;

import org.caffinitas.ohc.CacheSerializer;

import java.nio.ByteBuffer;

public class RecordMetaDataSerializer implements CacheSerializer<RecordMetaData> {

    public void serialize(RecordMetaData recordMetaData, ByteBuffer byteBuffer) {
        byteBuffer.putInt(recordMetaData.fileId);
        byteBuffer.putLong(recordMetaData.offset);
        byteBuffer.putInt(recordMetaData.recordSize);
        byteBuffer.flip();
    }

    public RecordMetaData deserialize(ByteBuffer byteBuffer) {
        int fileId = byteBuffer.getInt();
        long offset = byteBuffer.getLong();
        int size = byteBuffer.getInt();

        return new RecordMetaData(fileId, offset, size);
    }

    public int serializedSize(RecordMetaData recordMetaData) {
        return 4 + 8 + 4;
    }
}
