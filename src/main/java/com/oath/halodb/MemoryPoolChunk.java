/*
 * Copyright 2018, Oath Inc
 * Licensed under the terms of the Apache License 2.0. Please refer to accompanying LICENSE file for terms.
 */

package com.oath.halodb;

import static com.oath.halodb.MemoryPoolHashEntries.ENTRY_OFF_NEXT_CHUNK_INDEX;
import static com.oath.halodb.MemoryPoolHashEntries.ENTRY_OFF_NEXT_CHUNK_OFFSET;
import static com.oath.halodb.MemoryPoolHashEntries.HEADER_SIZE;

/**
 * Memory pool is divided into chunks of configurable sized slots. This represents such a chunk.
 *
 * All slots are members of linked lists, where the 'next' pointer is the first
 * element in the slot.
 *
 * Slots come in two varieties.  If the slot is storing an entry that has a key
 * that is less than or equal to the fixedKeyLength, then the key and data all
 * fit in one slot.  In this case, the slot is as follows:
 *
 * 5 bytes -- MemoryPoolAddress pointer (next)
 * 5 bytes -- HashEntry sizes (key/value length)
 * fixedKeyLength bytes -- key data
 * 16 bytes -- HashEntry location data (fileId, fileOffset, sequenceId)
 *
 * If the key is larger than fixedKeyLength bytes, then the data is stored in multiple
 * slots in the list, chained together.  The remainder of the key 'overflows' into
 * additional slots structured as follows:
 *
 * 5 bytes -- MemoryPoolAddress pointer (next)
 * remaining slot bytes -- key fragment
 *
 * The number of slots that a key of size K requires is
 *
 * 1 + (K - fixedKeyLength)/(21 + fixedKeyLength)
 *
 * For example, if fixedKeyLength is 8 bytes, a 60 byte key would require 3 slots:
 * 1 + (60 - 8)/(21 + 8) = 1 + (52/29) = 3
 *
 * If the fixedKeyLength was 20, a 60 byte key would require 2 slots:
 * 1 + (60 - 20)/(21 + 20) = 1 + (40 / 41) = 2
 *
 */
class MemoryPoolChunk<E extends HashEntry> {

    private static final int sizesOffset = HEADER_SIZE;

    private final int chunkSize;
    private final long address;
    private final int fixedKeyLength;
    private final int fixedKeyOffset;
    private final int locationOffset;
    private final int slotSize;
    private int writeOffset = 0;
    private final HashEntrySerializer<E> serializer;

    private MemoryPoolChunk(long address, int chunkSize, int fixedKeyLength, HashEntrySerializer<E> serializer) {
        this.address = address;
        this.chunkSize = chunkSize;
        this.fixedKeyLength = fixedKeyLength;
        this.fixedKeyOffset = sizesOffset + serializer.sizesSize();
        this.locationOffset = fixedKeyOffset + fixedKeyLength;
        this.slotSize = locationOffset + serializer.locationSize();
        this.serializer = serializer;
    }

    static <E extends HashEntry> MemoryPoolChunk<E> create(int chunkSize, int fixedKeyLength, HashEntrySerializer<E> serializer) {
        int fixedSlotSize = MemoryPoolHashEntries.slotSize(fixedKeyLength, serializer);
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
        Uns.putByte(address, slotOffset + ENTRY_OFF_NEXT_CHUNK_INDEX, (byte) next.chunkIndex);
        Uns.putInt(address, slotOffset + ENTRY_OFF_NEXT_CHUNK_OFFSET, next.chunkOffset);
    }

    int allocateSlot() {
        if (writeOffset + slotSize > chunkSize) {
            throw new IllegalStateException("can not allocate a slot when already full");
        }
        int writtenOffset = writeOffset;
        writeOffset += slotSize;
        return writtenOffset;
    }

    /**
     * Absolute put method. Writes to the slot pointed to by the offset.
     */
    void fillSlot(int slotOffset, byte[] key, E entry, MemoryPoolAddress nextAddress) {
        validateSlotOffset(slotOffset);
        // pointer to next slot
        setNextAddress(slotOffset, nextAddress);
        // key and value sizes
        entry.serializeSizes(sizeAddress(slotOffset));
        // key data, in fixed slot
        setKey(slotOffset, key, Math.min(key.length, fixedKeyLength));
        // entry metadata
        entry.serializeLocation(locationAddress(slotOffset));
    }

    void fillOverflowSlot(int slotOffset, byte[] key, int keyoffset, int len, MemoryPoolAddress nextAddress) {
        validateSlotOffset(slotOffset);
        //poiner to next slot
        setNextAddress(slotOffset, nextAddress);
        // set key data
        setExtendedKey(slotOffset, key, keyoffset, len);
    }

    void setEntry(int slotOffset, E entry) {
        entry.serializeSizes(sizeAddress(slotOffset));
        entry.serializeLocation(locationAddress(slotOffset));
    }

    int getWriteOffset() {
        return writeOffset;
    }

    boolean isFull() {
        return remaining() < slotSize;
    }

    int remaining() {
        return chunkSize - writeOffset;
    }

    E readEntry(int slotOffset) {
        return serializer.deserialize(sizeAddress(slotOffset), locationAddress(slotOffset));
    }

    private long sizeAddress(int slotOffset) {
        return address + slotOffset + sizesOffset;
    }

    private long locationAddress(int slotOffset) {
        return address + slotOffset + locationOffset;
    }

    private long keyAddress(int slotOffset) {
        return address + slotOffset + fixedKeyOffset;
    }

    private long extendedKeyAddress(int slotOffset) {
        return sizeAddress(slotOffset);
    }

    private void setKey(int slotOffset, byte[] key, int len) {
        if (len > fixedKeyLength) {
            throw new IllegalArgumentException("Invalid key write beyond fixedKeyLength, length - " + len);
        }
        Uns.copyMemory(key, 0, keyAddress(slotOffset), 0, len);
    }

    private void setExtendedKey(int slotOffset, byte[] key, int keyoffset, int len) {
        if (len > slotSize - sizesOffset) {
            throw new IllegalArgumentException("Invalid key write beyond slot with extended key, length - " + len);
        }
        Uns.copyMemory(key, keyoffset, extendedKeyAddress(slotOffset), 0, len);
    }

    long computeFixedKeyHash(int slotOffset, Hasher hasher, int keySize) {
        return hasher.hash(keyAddress(slotOffset), 0, keySize);
    }

    void copyEntireFixedKey(int slotOffset, long destinationAddress) {
        Uns.copyMemory(keyAddress(slotOffset), 0, destinationAddress, 0, fixedKeyLength);
    }

    int copyExtendedKey(int slotOffset, long destinationAddress, int destinationOffset, int len) {
        int copied = Math.min(len, slotSize - sizesOffset);
        Uns.copyMemory(extendedKeyAddress(slotOffset), 0, destinationAddress, destinationOffset, copied);
        return copied;
    }

    boolean compareKeyLength(int slotOffset, byte[] key) {
        validateSlotOffset(slotOffset);
        return key.length == getKeyLength(slotOffset);
    }

    boolean compareFixedKey(int slotOffset, byte[] key, int len) {
        validateSlotOffset(slotOffset);
        if (len > fixedKeyLength) {
            throw new IllegalArgumentException("Invalid request. key fragment larger than fixedKeyLength - " + len);
        }
        return compare(keyAddress(slotOffset), key, 0, len);
    }

    boolean compareExtendedKey(int slotOffset, byte[] key, int keyoffset, int len) {
        validateSlotOffset(slotOffset);
        if (len > fixedKeyLength + serializer.entrySize()) {
            throw new IllegalArgumentException("Invalid request. key fragment larger than slot capacity - " + len);
        }
        return compare(extendedKeyAddress(slotOffset), key, keyoffset, len);
    }

    boolean compareEntry(int slotOffset, E entry) {
        validateSlotOffset(slotOffset);
        return entry.compare(sizeAddress(slotOffset), locationAddress(slotOffset));
    }

    private boolean compare(long address, byte[] array, int arrayoffset, int len) {
        int p = 0, length = len;
        for (; length - p >= 8; p += 8) {
            if (Uns.getLong(address, p) != Uns.getLongFromByteArray(array, p + arrayoffset)) {
                return false;
            }
        }
        for (; length - p >= 4; p += 4) {
            if (Uns.getInt(address, p) != Uns.getIntFromByteArray(array, p + arrayoffset)) {
                return false;
            }
        }
        for (; length - p >= 2; p += 2) {
            if (Uns.getShort(address, p) != Uns.getShortFromByteArray(array, p + arrayoffset)) {
                return false;
            }
        }
        for (; length - p >= 1; p += 1) {
            if (Uns.getByte(address, p) != array[p + arrayoffset]) {
                return false;
            }
        }
        return true;
    }

    void validateSlotOffset(int slotOffset) {
        if (slotOffset + slotSize > chunkSize) {
            throw new IllegalArgumentException("Invalid request. slotOffset - " + slotOffset + " chunkSize - " + chunkSize);
        }
    }

    short getKeyLength(int slotOffset) {
        return serializer.readKeySize(sizeAddress(slotOffset));
    }
}
