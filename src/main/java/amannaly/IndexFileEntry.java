package amannaly;

import java.nio.ByteBuffer;

/**
 * @author Arjun Mannaly
 */
class IndexFileEntry {

    /**
     * Key size         - 1 bytes.
     * record size      - 4 bytes.
     * record offset    - 4 bytes.
     * sequence number  - 8 bytes
     * flags            - 1 byte.
     */
    final static int INDEX_FILE_HEADER_SIZE = 18;

    static final int KEY_SIZE_OFFSET = 0;
    static final int RECORD_SIZE_OFFSET = 1;
    static final int RECORD_OFFSET = 5;
    static final int SEQUENCE_NUMBER_OFFSET = 9;
    static final int FLAG_OFFSET = 17;


    private final byte[] key;
    private final int recordSize;
    private final int recordOffset;
    private final byte keySize;
    private final long sequenceNumber;
    private final byte flags;

    IndexFileEntry(byte[] key, int recordSize, int recordOffset, long sequenceNumber, byte flags) {
        this.key = key;
        this.recordSize = recordSize;
        this.recordOffset = recordOffset;
        this.flags = flags;
        this.sequenceNumber = sequenceNumber;

        this.keySize = (byte)key.length;
    }

    ByteBuffer[] serialize() {
        byte[] header = new byte[INDEX_FILE_HEADER_SIZE];
        ByteBuffer h = ByteBuffer.wrap(header);

        h.put(KEY_SIZE_OFFSET, keySize);
        h.putInt(RECORD_SIZE_OFFSET, recordSize);
        h.putInt(RECORD_OFFSET, recordOffset);
        h.putLong(SEQUENCE_NUMBER_OFFSET, sequenceNumber);
        h.put(FLAG_OFFSET, flags);

        return new ByteBuffer[] { h, ByteBuffer.wrap(key) };
    }

    static IndexFileEntry deserialize(ByteBuffer buffer) {
        byte keySize = buffer.get();
        int recordSize = buffer.getInt();
        int offset = buffer.getInt();
        long sequenceNumber = buffer.getLong();
        byte flag = buffer.get();

        byte[] key = new byte[keySize];
        buffer.get(key);

        return new IndexFileEntry(key, recordSize, offset, sequenceNumber, flag);
    }

    byte[] getKey() {
        return key;
    }

    int getRecordSize() {
        return recordSize;
    }

    int getRecordOffset() {
        return recordOffset;
    }

    byte getFlags() {
        return flags;
    }

    /**
     * tombstones will have the lsb of flags set to 1
     */
    boolean isTombStone() {
        return (flags & 1) == 1;
    }

    long getSequenceNumber() {
        return sequenceNumber;
    }
}
