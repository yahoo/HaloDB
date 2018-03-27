package com.oath.halodb;

import java.nio.ByteBuffer;

/**
 * This is what is stored in the tombstone file.
 *
 * @author Arjun Mannaly
 */
class TombstoneEntry {
    //TODO: test.

    /**
     * Key size         - 1 byte
     * Sequence number  - 8 byte
     */
    static final int TOMBSTONE_ENTRY_HEADER_SIZE = 1 + 8;

    private static final int sequenceNumberOffset = 0;
    private static final int keySizeOffset = 8;

    private final byte[] key;
    private final long sequenceNumber;

    TombstoneEntry(byte[] key, long sequenceNumber) {
        this.key = key;
        this.sequenceNumber = sequenceNumber;
    }

    byte[] getKey() {
        return key;
    }

    long getSequenceNumber() {
        return sequenceNumber;
    }

    ByteBuffer[] serialize() {
        byte keySize = (byte)key.length;
        ByteBuffer header = ByteBuffer.allocate(TOMBSTONE_ENTRY_HEADER_SIZE);
        header.putLong(sequenceNumberOffset, sequenceNumber);
        header.put(keySizeOffset, keySize);

        return new ByteBuffer[] {header, ByteBuffer.wrap(key)};
    }

    static TombstoneEntry deserialize(ByteBuffer buffer) {
        long sequenceNumber = buffer.getLong();
        int keySize = (int)buffer.get();
        byte[] key = new byte[keySize];
        buffer.get(key);

        return new TombstoneEntry(key, sequenceNumber);
    }
}
