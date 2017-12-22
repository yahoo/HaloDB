
package amannaly;

/**
 * @author Arjun Mannaly
 */
public class RecordMetaDataForCache {

	private final int fileId;
	private final long offset;
	private final int recordSize;
	private final long sequenceNumber;

	public RecordMetaDataForCache(int fileId, long offset, int recordSize, long sequenceNumber) {
		this.fileId = fileId;
		this.offset = offset;
		this.recordSize = recordSize;
		this.sequenceNumber = sequenceNumber;
	}

    public int getFileId() {
        return fileId;
    }

    public long getOffset() {
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
