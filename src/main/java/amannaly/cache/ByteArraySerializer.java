package amannaly.cache;

import amannaly.ohc.CacheSerializer;

import java.nio.ByteBuffer;

/**
 * @author Arjun Mannaly
 */
public class ByteArraySerializer implements CacheSerializer<byte[]> {

    @Override
    public void serialize(byte[] value, ByteBuffer buf) {
        buf.put(value);
    }

    @Override
    public byte[] deserialize(ByteBuffer buf) {
        return buf.array();
    }

    @Override
    public int serializedSize(byte[] value) {
        return value.length;
    }
}
