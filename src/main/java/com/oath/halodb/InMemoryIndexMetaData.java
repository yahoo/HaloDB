
/*
 * Copyright 2018, Oath Inc
 * Licensed under the terms of the Apache License 2.0. Please refer to accompanying LICENSE file for terms.
 */

package com.oath.halodb;

/**
 * Metadata stored in the in-memory index for each key.
 */
class InMemoryIndexMetaData extends HashEntry implements HashEntryLocation {

    private final int fileId;
    private final int valueOffset;
    private final long sequenceNumber;

    /*
     * From HashEntry:
     * key and value size - 5 bytes, 11 bits for key size and 29 bits for value size
     *
     * Additionally in this class, value meta:
     * file id            - 4 bytes
     * value offset       - 4 bytes
     * sequence number    - 8 bytes
     */
    static final int VALUE_META_SIZE = 4 + 4 + 8;

    InMemoryIndexMetaData(int fileId, int valueOffset, int valueSize, long sequenceNumber, int keySize) {
        super(keySize, valueSize);
        this.fileId = fileId;
        this.valueOffset = valueOffset;
        this.sequenceNumber = sequenceNumber;
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

    @Override
    public int getFileId() {
        return fileId;
    }

    @Override
    public int getValueOffset() {
        return valueOffset;
    }

    @Override
    public long getSequenceNumber() {
        return sequenceNumber;
    }

    @Override
    final boolean compareLocation(long address) {
        return HashEntryLocation.compareLocation(address, getFileId(), getValueOffset(), getSequenceNumber());
    }

    @Override
    final void serializeLocation(long address) {
        HashEntryLocation.serializeLocation(address, getFileId(), getValueOffset(), getSequenceNumber());
    }

    InMemoryIndexMetaData relocated(int newFileId, int newWriteFileOffset) {
        int newOffset = RecordEntry.getValueOffset(newWriteFileOffset, getKeySize());
        return new InMemoryIndexMetaData(newFileId, newOffset, getValueSize(), sequenceNumber, getKeySize());
    }
}
