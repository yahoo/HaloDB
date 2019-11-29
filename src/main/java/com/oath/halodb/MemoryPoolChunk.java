/*
 * Copyright 2018, Oath Inc
 * Licensed under the terms of the Apache License 2.0. Please refer to accompanying LICENSE file for terms.
 */

package com.oath.halodb;

import static com.oath.halodb.MemoryPoolHashEntries.ENTRY_OFF_NEXT_CHUNK_INDEX;
import static com.oath.halodb.MemoryPoolHashEntries.ENTRY_OFF_NEXT_CHUNK_OFFSET;
import static com.oath.halodb.MemoryPoolHashEntries.HEADER_SIZE;

/**
 * Memory pool is divided into chunks of configurable size. This represents such a chunk.
 */
class MemoryPoolChunk<E extends HashEntry> {

    private final long address;
    private final int chunkSize;
    private final int fixedKeyLength;
    private final int fixedEntryLength;
    private final int fixedSlotSize;
    private int writeOffset = 0;
    private final HashEntrySerializer<E> serializer;

    private MemoryPoolChunk(long address, int chunkSize, int fixedKeyLength, HashEntrySerializer<E> serializer) {
        this.address = address;
        this.chunkSize = chunkSize;
        this.fixedKeyLength = fixedKeyLength;
        this.fixedEntryLength = serializer.entrySize();
        this.fixedSlotSize = HEADER_SIZE + fixedKeyLength + fixedEntryLength;
        this.serializer = serializer;
    }

    static <E extends HashEntry> MemoryPoolChunk<E> create(int chunkSize, int fixedKeyLength, HashEntrySerializer<E> serializer) {
        int fixedSlotSize = HEADER_SIZE + fixedKeyLength + serializer.entrySize();
        if (fixedSlotSize > chunkSize) {
            throw new IllegalArgumentException("fixedSlotSize " + fixedSlotSize + " must be smaller than chunkSize " + chunkSize);
        }
        long address = Uns.allocate(chunkSize, true);
        return new MemoryPoolChunk<>(address, chunkSize, fixedKeyLength, serializer);
    }

    void destroy() {
        Uns.free(address);
    }

    MemoryPoolAddress getNextAddress(int slotOffset) {
        byte chunkIndex = Uns.getByte(address, slotOffset + ENTRY_OFF_NEXT_CHUNK_INDEX);
        int chunkOffset = Uns.getInt(address, slotOffset + ENTRY_OFF_NEXT_CHUNK_OFFSET);

        return new MemoryPoolAddress(chunkIndex, chunkOffset);
    }

    void setNextAddress(int slotOffset, MemoryPoolAddress next) {
        Uns.putByte(address, slotOffset + ENTRY_OFF_NEXT_CHUNK_INDEX, next.chunkIndex);
        Uns.putInt(address, slotOffset + ENTRY_OFF_NEXT_CHUNK_OFFSET, next.chunkOffset);
    }

    /**
     * Relative put method. Writes to the slot pointed to by the writeOffset and increments the writeOffset.
     */
    void fillNextSlot(byte[] key, E entry, MemoryPoolAddress nextAddress) {
        fillSlot(writeOffset, key, entry, nextAddress);
        writeOffset += fixedSlotSize;
    }

    /**
     * Absolute put method. Writes to the slot pointed to by the offset.
     */
    void fillSlot(int slotOffset, byte[] key, E entry, MemoryPoolAddress nextAddress) {
        if (key.length > fixedKeyLength) {
            throw new IllegalArgumentException(
                String.format("Invalid request. Key length %d. fixed key length %d",
                              key.length, fixedKeyLength)
            );
        }
        if (chunkSize - slotOffset < fixedSlotSize) {
            throw new IllegalArgumentException(
                String.format("Invalid offset %d. Chunk size %d. fixed slot size %d",
                              slotOffset, chunkSize, fixedSlotSize)
            );
        }

        // pointer to next slot
        setNextAddress(slotOffset, nextAddress);
        // key data, in fixed slot
        setKey(slotOffset, key);
        // entry metadata
        setEntry(slotOffset, entry);
    }

    int getWriteOffset() {
        return writeOffset;
    }

    int remaining() {
        return chunkSize - writeOffset;
    }

    E readEntry(int slotOffset) {
        long sizeAddress = entryAddress(slotOffset);
        long locationAddress = sizeAddress + serializer.sizesSize();
        return serializer.deserialize(sizeAddress, locationAddress);
    }

    private long entryAddress(int slotOffset) {
        return address + slotOffset + HEADER_SIZE + fixedKeyLength;
    }

    void setEntry(int slotOffset, E entry) {
        long sizeAddress = entryAddress(slotOffset);
        long locationAddress = sizeAddress + serializer.sizesSize();
        entry.serializeSizes(sizeAddress);
        entry.serializeLocation(locationAddress);
    }

    private long keyAddress(int slotOffset) {
        return address + slotOffset + HEADER_SIZE;
    }

    private void setKey(int slotOffset, byte[] key) {
        Uns.copyMemory(key, 0, keyAddress(slotOffset), 0, key.length);
    }

    long computeHash(int slotOffset, Hasher hasher) {
        return hasher.hash(keyAddress(slotOffset), 0, getKeyLength(slotOffset));
    }

    boolean compareKey(int slotOffset, byte[] key) {
        if (key.length > fixedKeyLength || slotOffset + fixedSlotSize > chunkSize) {
            throw new IllegalArgumentException("Invalid request. slotOffset - " + slotOffset + " key.length - " + key.length);
        }
       return getKeyLength(slotOffset) == key.length && compare(slotOffset + HEADER_SIZE, key);
    }

    boolean compareEntry(int slotOffset, E entry) {
        if (slotOffset + fixedSlotSize > chunkSize) {
            throw new IllegalArgumentException("Invalid request. slotOffset - " + slotOffset + " chunkSize - " + chunkSize);
        }
        long sizeAddress = entryAddress(slotOffset);
        long locationAddress = sizeAddress + serializer.sizesSize();
        return entry.compare(sizeAddress, locationAddress);
    }

    private boolean compare(int offset, byte[] array) {
        int p = 0, length = array.length;
        for (; length - p >= 8; p += 8) {
            if (Uns.getLong(address, offset + p) != Uns.getLongFromByteArray(array, p)) {
                return false;
            }
        }
        for (; length - p >= 4; p += 4) {
            if (Uns.getInt(address, offset + p) != Uns.getIntFromByteArray(array, p)) {
                return false;
            }
        }
        for (; length - p >= 2; p += 2) {
            if (Uns.getShort(address, offset + p) != Uns.getShortFromByteArray(array, p)) {
                return false;
            }
        }
        for (; length - p >= 1; p += 1) {
            if (Uns.getByte(address, offset + p) != array[p]) {
                return false;
            }
        }
        return true;
    }

    private short getKeyLength(int slotOffset) {
        return serializer.readKeySize(entryAddress(slotOffset));
    }
}
