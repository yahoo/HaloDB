package amannaly;

import com.google.protobuf.ByteString;

import java.nio.ByteBuffer;

public class HintFileEntry {

    /**
     * Key size      - 2 bytes.
     * record size   - 4 bytes.
     * record offset - 8 bytes.
     */
    public final static int HINT_FILE_HEADER_SIZE = 14;

    private final ByteString key;
    private final int recordSize;
    private final long recordOffset;
    private final short keySize;

    public HintFileEntry(ByteString key, int recordSize, long recordOffset) {
        this.key = key;
        this.recordSize = recordSize;
        this.recordOffset = recordOffset;

        this.keySize = (short)key.size();
    }

    public ByteBuffer[] serialize() {
        byte[] header = new byte[HINT_FILE_HEADER_SIZE];
        ByteBuffer h = ByteBuffer.wrap(header);

        h.putShort(keySize);
        h.putInt(recordSize);
        h.putLong(recordOffset);

        h.flip();

        return new ByteBuffer[] { h, key.asReadOnlyByteBuffer() };
    }

    public ByteString getKey() {
        return key;
    }

    public int getRecordSize() {
        return recordSize;
    }

    public long getRecordOffset() {
        return recordOffset;
    }
}
