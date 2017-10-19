package amannaly;

import java.nio.ByteBuffer;
import java.util.Arrays;

public class Record {

    public static final int KEY_SIZE_OFFSET = 0;
    public static final int VALUE_SIZE_OFFSET = 2;

    /**
     * short key length: 2 bytes.
     * int value length: 4 bytes.
     */
    public static final int HEADER_SIZE = 6;

    private final byte[] key, value;

    private final int recordSize;

    private RecordMetaData recordMetaData;

    public Record(byte[] key, byte[] value) {
        this.key = key;
        this.value = value;

        this.recordSize = key.length + value.length + HEADER_SIZE;
    }

    public ByteBuffer[] serialize() {
        byte[] header = new byte[HEADER_SIZE];

        ByteBuffer headerBuffer = ByteBuffer.wrap(header);
        headerBuffer.putShort((short) key.length);
        headerBuffer.putInt(value.length);
        headerBuffer.flip();

        return new ByteBuffer[] {headerBuffer, ByteBuffer.wrap(key), ByteBuffer.wrap(value)};
    }

    public byte[] getKey() {
        return key;
    }

    public byte[] getValue() {
        return value;
    }

    public RecordMetaData getRecordMetaData() {
        return recordMetaData;
    }

    public void setRecordMetaData(RecordMetaData recordMetaData) {
        this.recordMetaData = recordMetaData;
    }

    public int getRecordSize() {
        return recordSize;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (!(obj instanceof Record)) {
            return false;
        }

        Record record = (Record)obj;

        if (getKey() == null || record.getKey() == null)
            return false;

        if (getValue() == null || record.getValue() == null)
            return false;

        return Arrays.equals(getKey(), record.getKey()) && Arrays.equals(getValue(), record.getValue());
    }

    //TODO: override hash code.
}
