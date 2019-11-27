
/*
 * Copyright 2018, Oath Inc
 * Licensed under the terms of the Apache License 2.0. Please refer to accompanying LICENSE file for terms.
 */

package com.oath.halodb;

/**
 * Metadata stored in the in-memory index for each key.
 */
class InMemoryIndexMetaData implements HashEntry {

    private final int fileId;
    private final int valueOffset;
    private final int valueSize;
    private final long sequenceNumber;
    private final short keySize;

    /*
     * key and value size - 5 bytes, 11 bits for key size and 29 bits for value size
     * file id            - 4 bytes
     * value offset       - 4 bytes
     * sequence number    - 8 bytes
     */
    static final int SERIALIZED_SIZE = 5 + 4 + 4 + 8;

    InMemoryIndexMetaData(int fileId, int valueOffset, int valueSize, long sequenceNumber, int keySize) {
        this.fileId = fileId;
        this.valueOffset = valueOffset;
        this.valueSize = Utils.validateValueSize(valueSize);;
        this.sequenceNumber = sequenceNumber;
        this.keySize = Utils.validateKeySize(keySize);
    }

    InMemoryIndexMetaData(IndexFileEntry entry, int fileId) {
        this(fileId,
             RecordEntry.getValueOffset(entry.getRecordOffset(), entry.getKey().length),
             RecordEntry.getValueSize(entry.getRecordSize(), entry.getKey().length),
             entry.getSequenceNumber(),
             entry.getKey().length);
    }

    InMemoryIndexMetaData(RecordEntry.Header header, int fileId, int offset) {
        this(fileId,
             RecordEntry.getValueOffset(offset, header.getKeySize()),
             header.getValueSize(),
             header.getSequenceNumber(),
             header.getKeySize());
    }

    void serialize(long address) {
        Uns.putInt(address, 0, keySize | valueSize << 11);
        Uns.putByte(address, 4, (byte) (valueSize >>> 21));
        Uns.putInt(address, 5, fileId);
        Uns.putInt(address, 9, valueOffset);
        Uns.putLong(address, 13, sequenceNumber);
    }

    public static InMemoryIndexMetaData deserialize(long address) {
        int firstWord = Uns.getInt(address, 0);
        byte nextByte = Uns.getByte(address, 4);
        short keySize = extractKeySize(firstWord);
        int valueSize = extractValueSize(firstWord, nextByte);
        int fileId = Uns.getInt(address, 5);
        int offset = Uns.getInt(address, 9);
        long sequenceNumber = Uns.getLong(address, 13);

        return new InMemoryIndexMetaData(fileId, offset, valueSize, sequenceNumber, keySize);
    }

    static short getKeySize(long address) {
        return extractKeySize(Uns.getInt(address, 0));
    }

    private static short extractKeySize(int firstWord) {
        return (short) (firstWord & 0b0111_1111_1111);
    }

    private static int extractValueSize(int firstWord, byte nextByte) {
        return (firstWord >>> 11) | nextByte << 21;
    }

    public boolean compare(long address) {
        int firstWord = Uns.getInt(address, 0);
        return keySize == extractKeySize(firstWord)
            && valueSize == extractValueSize(firstWord, Uns.getByte(address, 4))
            && fileId == Uns.getInt(address, 5)
            && valueOffset == Uns.getInt(address, 9)
            && sequenceNumber == Uns.getLong(address, 13);
    }

    @Override
    public short getKeySize() {
        return keySize;
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

    public InMemoryIndexMetaData relocated(int newFileId, int newWriteFileOffset) {
        int newOffset = RecordEntry.getValueOffset(newWriteFileOffset, keySize);
        return new InMemoryIndexMetaData(newFileId, newOffset, valueSize, sequenceNumber, keySize);
    }
}
