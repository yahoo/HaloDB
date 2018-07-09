/*
 * Copyright 2018, Oath Inc
 * Licensed under the terms of the Apache License 2.0. Please refer to accompanying LICENSE file for terms.
 */

package com.oath.halodb;

import java.nio.ByteBuffer;

import static com.oath.halodb.MemoryPoolHashEntries.*;

/**
 * Memory pool is divided into chunks of configurable size. This represents such a chunk.
 *
 * @author Arjun Mannaly
 */
class MemoryPoolChunk {

    private final long address;
    private final int chunkSize;
    private final int fixedKeyLength;
    private final int fixedValueLength;
    private final int fixedSlotSize;
    private int writeOffset = 0;

    private MemoryPoolChunk(long address, int chunkSize, int fixedKeyLength, int fixedValueLength) {
        this.address = address;
        this.chunkSize = chunkSize;
        this.fixedKeyLength = fixedKeyLength;
        this.fixedValueLength = fixedValueLength;
        this.fixedSlotSize = HEADER_SIZE + fixedKeyLength + fixedValueLength;
    }

    static MemoryPoolChunk create(int chunkSize, int fixedKeyLength, int fixedValueLength) {
        int fixedSlotSize = HEADER_SIZE + fixedKeyLength + fixedValueLength;
        if (fixedSlotSize > chunkSize) {
            throw new IllegalArgumentException("fixedSlotSize " + fixedSlotSize + " must be smaller than chunkSize " + chunkSize);
        }
        long address = Uns.allocate(chunkSize, true);
        return new MemoryPoolChunk(address, chunkSize, fixedKeyLength, fixedValueLength);
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
    void fillNextSlot(byte[] key, byte[] value, MemoryPoolAddress nextAddress) {
        fillSlot(writeOffset, key, value, nextAddress);
        writeOffset += fixedSlotSize;
    }

    /**
     * Absolute put method. Writes to the slot pointed to by the offset.
     */
    void fillSlot(int slotOffset, byte[] key, byte[] value, MemoryPoolAddress nextAddress) {
        if (key.length > fixedKeyLength || value.length != fixedValueLength) {
            throw new IllegalArgumentException(
                String.format("Invalid request. Key length %d. fixed key length %d. Value length %d",
                              key.length, fixedKeyLength, value.length)
            );
        }
        if (chunkSize - slotOffset < fixedSlotSize) {
            throw new IllegalArgumentException(
                String.format("Invalid offset %d. Chunk size %d. fixed slot size %d",
                              slotOffset, chunkSize, fixedSlotSize)
            );
        }

        setNextAddress(slotOffset, nextAddress);
        Uns.putByte(address, slotOffset + ENTRY_OFF_KEY_LENGTH, (byte) key.length);
        Uns.copyMemory(key, 0, address, slotOffset + ENTRY_OFF_DATA, key.length);
        setValue(value, slotOffset);
    }

    void setValue(byte[] value, int slotOffset) {
        if (value.length != fixedValueLength) {
            throw new IllegalArgumentException(
                String.format("Invalid value length. fixedValueLength %d, value length %d",
                              fixedValueLength, value.length)
            );
        }

        Uns.copyMemory(value, 0, address, slotOffset + ENTRY_OFF_DATA + fixedKeyLength, value.length);
    }

    int getWriteOffset() {
        return writeOffset;
    }

    int remaining() {
        return chunkSize - writeOffset;
    }

    ByteBuffer readOnlyValueByteBuffer(int offset) {
        return Uns.directBufferFor(address, offset + ENTRY_OFF_DATA + fixedKeyLength, fixedValueLength, true);
    }

    ByteBuffer readOnlyKeyByteBuffer(int offset) {
        return Uns.directBufferFor(address, offset + ENTRY_OFF_DATA, getKeyLength(offset), true);
    }

    long computeHash(int slotOffset, Hasher hasher) {
        return hasher.hash(address, slotOffset + ENTRY_OFF_DATA, getKeyLength(slotOffset));
    }


    boolean compareKey(int slotOffset, byte[] key) {
        if (key.length > fixedKeyLength || slotOffset + fixedSlotSize > chunkSize) {
            throw new IllegalArgumentException("Invalid request. slotOffset - " + slotOffset + " key.length - " + key.length);
        }

        return getKeyLength(slotOffset) == key.length && compare(slotOffset + ENTRY_OFF_DATA, key);
    }

    boolean compareValue(int slotOffset, byte[] value) {
        if (value.length != fixedValueLength || slotOffset + fixedSlotSize > chunkSize) {
            throw new IllegalArgumentException("Invalid request. slotOffset - " + slotOffset + " value.length - " + value.length);
        }

        return compare(slotOffset + ENTRY_OFF_DATA + fixedKeyLength, value);
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

    private byte getKeyLength(int slotOffset) {
        return Uns.getByte(address, slotOffset + ENTRY_OFF_KEY_LENGTH);
    }
}
