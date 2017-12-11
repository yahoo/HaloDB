package amannaly.cache;

import amannaly.ohc.CacheSerializer;

import java.nio.ByteBuffer;

/**
 * @author Arjun Mannaly
 */
public class SequenceNumberSerializer implements CacheSerializer<Long> {

    @Override
    public void serialize(Long sequenceNumber, ByteBuffer byteBuffer) {
        byteBuffer.putLong(sequenceNumber);
        byteBuffer.flip();
    }

    @Override
    public Long deserialize(ByteBuffer byteBuffer) {
        return byteBuffer.getLong();
    }

    @Override
    public int serializedSize(Long sequenceNumber) {
        return 8;
    }
}
