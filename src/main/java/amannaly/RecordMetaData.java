
package amannaly;

public class RecordMetaData {

	public final int fileId;
	public final long offset;
	public final int recordSize;

	public RecordMetaData(int fileId, long offset, int recordSize) {
		this.fileId = fileId;
		this.offset = offset;
		this.recordSize = recordSize;
	}

	//TODO: need to define equals and hash code as it is used in KeyCache's replace operation.

}
