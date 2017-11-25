package amannaly;

import java.nio.ByteBuffer;

/**
 * @author Arjun Mannaly
 */
class IndexFileEntry {

    /**
     * Key size         - 2 bytes.
     * record size      - 4 bytes.
     * record offset    - 8 bytes.
     * sequence number  - 8 bytes
     * flags            - 1 byte.
     */
    public final static int INDEX_FILE_HEADER_SIZE = 23;

    public static final int KEY_SIZE_OFFSET = 0;
    public static final int RECORD_SIZE_OFFSET = 2;
    public static final int RECORD_OFFSET = 6;
    public static final int SEQUENCE_NUMBER_OFFSET = 14;
    public static final int FLAG_OFFSET = 22;


    private final byte[] key;
    private final int recordSize;
    private final long recordOffset;
    private final short keySize;
    private final long sequenceNumber;
    private final byte flags;

    IndexFileEntry(byte[] key, int recordSize, long recordOffset, long sequenceNumber, byte flags) {
        this.key = key;
        this.recordSize = recordSize;
        this.recordOffset = recordOffset;
        this.flags = flags;
        this.sequenceNumber = sequenceNumber;

        this.keySize = (short)key.length;
    }

    ByteBuffer[] serialize() {
        byte[] header = new byte[INDEX_FILE_HEADER_SIZE];
        ByteBuffer h = ByteBuffer.wrap(header);

        h.putShort(KEY_SIZE_OFFSET, keySize);
        h.putInt(RECORD_SIZE_OFFSET, recordSize);
        h.putLong(RECORD_OFFSET, recordOffset);
        h.putLong(SEQUENCE_NUMBER_OFFSET, sequenceNumber);
        h.put(FLAG_OFFSET, flags);

        return new ByteBuffer[] { h, ByteBuffer.wrap(key) };
    }

    static IndexFileEntry deserialize(ByteBuffer buffer) {
        short keySize = buffer.getShort();
        int recordSize = buffer.getInt();
        long offset = buffer.getLong();
        long sequenceNumber = buffer.getLong();
        byte flag = buffer.get();

        byte[] key = new byte[keySize];
        buffer.get(key);

        return new IndexFileEntry(key, recordSize, offset, sequenceNumber, flag);
    }

    public byte[] getKey() {
        return key;
    }

    public int getRecordSize() {
        return recordSize;
    }

    long getRecordOffset() {
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

    public long getSequenceNumber() {
        return sequenceNumber;
    }
}
