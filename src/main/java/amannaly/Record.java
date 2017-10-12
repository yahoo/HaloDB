package amannaly;

import com.google.protobuf.ByteString;

import java.nio.ByteBuffer;

public class Record {

    public static final int KEY_SIZE_OFFSET = 0;
    public static final int VALUE_SIZE_OFFSET = 2;

    /**
     * short key length: 2 bytes.
     * int value length: 4 bytes.
     */
    public static final int HEADER_SIZE = 6;

    private final ByteString key, value;

    private final int recordSize;

    private RecordMetaData recordMetaData;

    public Record(ByteString key, ByteString value) {
        this.key = key;
        this.value = value;

        this.recordSize = key.size() + value.size() + HEADER_SIZE;
    }

    public ByteBuffer[] serialize() {
        byte[] header = new byte[HEADER_SIZE];

        ByteBuffer headerBuffer = ByteBuffer.wrap(header);
        headerBuffer.putShort((short) key.size());
        headerBuffer.putInt(value.size());
        headerBuffer.flip();

        return new ByteBuffer[] {headerBuffer, key.asReadOnlyByteBuffer(), value.asReadOnlyByteBuffer()};
    }

    public ByteString getKey() {
        return key;
    }

    public ByteString getValue() {
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

        return getKey().equals(record.getKey()) && getValue().equals(record.getValue());
    }

    //TODO: override hash code.
}
