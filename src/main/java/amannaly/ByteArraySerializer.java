package amannaly;

import amannaly.cache.CacheSerializer;

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
        // Cannot use buf.array() as buf is read-only for get() operations.
        byte[] array = new byte[buf.remaining()];
        buf.get(array);
        return array;
    }

    @Override
    public int serializedSize(byte[] value) {
        return value.length;
    }
}
