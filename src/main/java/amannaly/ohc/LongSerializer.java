package amannaly.ohc;

import com.google.protobuf.ByteString;

import amannaly.Utils;

import org.caffinitas.ohc.CacheSerializer;

import java.nio.ByteBuffer;

public class LongSerializer implements CacheSerializer<ByteString> {

    @Override
    public void serialize(ByteString value, ByteBuffer buf) {
        buf.put(value.toByteArray());
    }

    @Override
    public int serializedSize(ByteString value) {
        return 8;
    }

    public ByteString deserialize(ByteBuffer buf) {
        return ByteString.copyFrom(Utils.longToBytes(buf.getLong()));
    }
}
