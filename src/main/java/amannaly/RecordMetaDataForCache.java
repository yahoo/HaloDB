
package amannaly;

import java.nio.ByteBuffer;

/**
 * @author Arjun Mannaly
 */
class RecordMetaDataForCache {

	private final int fileId;
	private final int valueOffset;
	private final int valueSize;
	private final long sequenceNumber;

    static final int SERIALIZED_SIZE = 4 + 4 + 4 + 8;

    RecordMetaDataForCache(int fileId, int valueOffset, int valueSize, long sequenceNumber) {
		this.fileId = fileId;
		this.valueOffset = valueOffset;
		this.valueSize = valueSize;
		this.sequenceNumber = sequenceNumber;
	}

    void serialize(ByteBuffer byteBuffer) {
        byteBuffer.putInt(getFileId());
        byteBuffer.putInt(getValueOffset());
        byteBuffer.putInt(getValueSize());
        byteBuffer.putLong(getSequenceNumber());
        byteBuffer.flip();
    }

    static RecordMetaDataForCache deserialize(ByteBuffer byteBuffer) {
        int fileId = byteBuffer.getInt();
        int offset = byteBuffer.getInt();
        int size = byteBuffer.getInt();
        long sequenceNumber = byteBuffer.getLong();

        return new RecordMetaDataForCache(fileId, offset, size, sequenceNumber);
    }

    int getFileId() {
        return fileId;
    }

    int getValueOffset() {
        return valueOffset;
    }

    int getValueSize() {
        return valueSize;
    }

    long getSequenceNumber() {
        return sequenceNumber;
    }

    //TODO: need to define equals and hash code as it is used in KeyCache's replace operation.

}
