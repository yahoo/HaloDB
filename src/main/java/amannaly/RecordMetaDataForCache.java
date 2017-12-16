
package amannaly;

/**
 * @author Arjun Mannaly
 */
public class RecordMetaDataForCache {

	private final int fileId;
	private final long offset;
	private final int recordSize;

	public RecordMetaDataForCache(int fileId, long offset, int recordSize) {
		this.fileId = fileId;
		this.offset = offset;
		this.recordSize = recordSize;
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

    //TODO: need to define equals and hash code as it is used in KeyCache's replace operation.

}
