package amannaly.ohc;

import com.google.protobuf.ByteString;

import org.caffinitas.ohc.CacheSerializer;

import java.nio.ByteBuffer;

public class ByteStringSerializer implements CacheSerializer<ByteString> {

    @Override
    public void serialize(ByteString value, ByteBuffer buf) {
        buf.put(value.toByteArray());
    }

    @Override
    public ByteString deserialize(ByteBuffer buf) {
        return ByteString.copyFrom(buf);
    }

    @Override
    public int serializedSize(ByteString value) {
        return value.size();
    }
}
