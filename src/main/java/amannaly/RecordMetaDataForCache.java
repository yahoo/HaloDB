
package amannaly;

import java.nio.ByteBuffer;

/**
 * @author Arjun Mannaly
 */
class RecordMetaDataForCache {

	private final int fileId;
	private final int offset;
	private final int recordSize;
	private final long sequenceNumber;

    static final int SERIALIZED_SIZE = 4 + 4 + 4 + 8;

    RecordMetaDataForCache(int fileId, int offset, int recordSize, long sequenceNumber) {
		this.fileId = fileId;
		this.offset = offset;
		this.recordSize = recordSize;
		this.sequenceNumber = sequenceNumber;
	}

    void serialize(ByteBuffer byteBuffer) {
        byteBuffer.putInt(getFileId());
        byteBuffer.putInt(getOffset());
        byteBuffer.putInt(getRecordSize());
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

    public int getFileId() {
        return fileId;
    }

    public int getOffset() {
        return offset;
    }

    public int getRecordSize() {
        return recordSize;
    }

    public long getSequenceNumber() {
        return sequenceNumber;
    }

    //TODO: need to define equals and hash code as it is used in KeyCache's replace operation.

}
