/*
 * Copyright 2018, Oath Inc
 * Licensed under the terms of the Apache License 2.0. Please refer to accompanying LICENSE file for terms.
 */

package com.oath.halodb;

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
 * 4 bytes -- MemoryPoolAddress pointer (next)
 * 5 bytes -- HashEntry sizes (key/value length)
 * fixedKeyLength bytes -- key data
 * 16 bytes -- HashEntry location data (fileId, fileOffset, sequenceId)
 *
 * If the key is larger than fixedKeyLength bytes, then the data is stored in multiple
 * slots in the list, chained together.  The remainder of the key 'overflows' into
 * additional slots structured as follows:
 *
 * 4 bytes -- MemoryPoolAddress pointer (next)
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

    private static final int sizesOffset = MemoryPoolAddress.ADDRESS_SIZE;

    private final int chunkId;
    private final long address;
    private final int fixedKeyLength;
    private final int fixedKeyOffset;
    private final int locationOffset;
    private final int slotSize;
    private final int slots;
    private final HashEntrySerializer<E> serializer;
    private int writeSlot = 0;
    private boolean destroyed = false;

    private MemoryPoolChunk(long address, int chunkId, int slots, int fixedKeyLength, HashEntrySerializer<E> serializer) {
        this.address = address;
        this.chunkId = chunkId;
        this.slots = slots;
        this.fixedKeyLength = fixedKeyLength;
        this.fixedKeyOffset = sizesOffset + serializer.sizesSize();
        this.locationOffset = fixedKeyOffset + fixedKeyLength;
        this.slotSize = MemoryPoolChunk.slotSize(fixedKeyLength, serializer);
        this.serializer = serializer;
    }

    static <E extends HashEntry> MemoryPoolChunk<E> create(int id, int chunkSize, int fixedKeyLength, HashEntrySerializer<E> serializer) {
        int fixedSlotSize = MemoryPoolChunk.slotSize(fixedKeyLength, serializer);
        int slots = Math.min((chunkSize / fixedSlotSize), MemoryPoolAddress.MAX_NUMBER_OF_SLOTS);
        if (slots < 1) {
            throw new IllegalArgumentException("fixedSlotSize " + fixedSlotSize + " must not be larger than chunkSize " + chunkSize);
        }
        long address = Uns.allocate(slots * fixedSlotSize);
        return new MemoryPoolChunk<>(address, id, slots, fixedKeyLength, serializer);
    }

    void destroy() {
        if (!destroyed) {
            Uns.free(address);
            destroyed = true;
        }
    }

    @Override
    protected void finalize() throws Throwable {
        destroy();
        super.finalize();
    }

    Slot slotFor(int slot) {
        return new Slot(slot);
    }

    Slot allocateSlot() {
        if (isFull()) {
            throw new IllegalStateException("can not allocate a slot when already full");
        }
        Slot slot = slotFor(writeSlot);
        writeSlot++;
        return slot;
    }

    int getWriteOffset() {
        return slotToOffset(writeSlot);
    }

    boolean isFull() {
        return writeSlot >= slots;
    }

    int remainingSlots() {
        return slots - writeSlot;
    }

    private int slotToOffset(int slot) {
        if (slot > slots) {
            throw new IllegalArgumentException("Invalid request. slot - " + slot + " total slots - " + slots);
        }
        return slot * slotSize;
    }

    public static int slotSize(int fixedKeySize, HashEntrySerializer<?> serializer) {
        return sizesOffset + fixedKeySize + serializer.entrySize();
    }

    /** Represents a valid Slot within a MemoryPoolChunk **/
    class Slot {
        private final int slot;
        private final int offset;
        private Slot(int slot) {
            this.slot = slot;
            this.offset = slotToOffset(slot);
        }

        short getKeyLength() {
            return serializer.readKeySize(sizeAddress());
        }

        int toAddress() {
            return MemoryPoolAddress.encode(chunkId, slot);
        }

        int getNextAddress() {
            return Uns.getInt(address, offset);
        }

        void setNextAddress(int nextAddress) {
            Uns.putInt(address, offset, nextAddress);
        }

        void fillSlot(byte[] key, E entry, int nextAddress) {
            // pointer to next slot
            setNextAddress(nextAddress);
            // key and value sizes
            entry.serializeSizes(sizeAddress());
            // key data, in fixed slot
            setKey(key, Math.min(key.length, fixedKeyLength));
            // entry metadata
            entry.serializeLocation(locationAddress());
        }

        void fillOverflowSlot(byte[] key, int keyoffset, int len, int nextAddress) {
            //poiner to next slot
            setNextAddress(nextAddress);
            // set key data
            setExtendedKey(key, keyoffset, len);
        }

        void setEntry(E entry) {
            entry.serializeSizes(sizeAddress());
            entry.serializeLocation(locationAddress());
        }

        E readEntry() {
            return serializer.deserialize(sizeAddress(), locationAddress());
        }

        long computeFixedKeyHash(Hasher hasher, int keySize) {
            return hasher.hash(keyAddress(), 0, keySize);
        }

        void copyEntireFixedKey(long destinationAddress) {
            Uns.copyMemory(keyAddress(), 0, destinationAddress, 0, fixedKeyLength);
        }

        int copyExtendedKey(long destinationAddress, int destinationOffset, int len) {
            int copied = Math.min(len, slotSize - sizesOffset);
            Uns.copyMemory(extendedKeyAddress(), 0, destinationAddress, destinationOffset, copied);
            return copied;
        }

        boolean compareFixedKey(byte[] key, int len) {
            if (len > fixedKeyLength) {
                throw new IllegalArgumentException("Invalid request. key fragment larger than fixedKeyLength: " + len);
            }
            return Uns.compare(keyAddress(), key, 0, len);
        }

        boolean compareExtendedKey(byte[] key, int keyoffset, int len) {
            if (len > fixedKeyLength + serializer.entrySize()) {
                throw new IllegalArgumentException("Invalid request. key fragment larger than slot capacity: " + len);
            }
            return Uns.compare(extendedKeyAddress(), key, keyoffset, len);
        }

        boolean compareEntry(E entry) {
            return entry.compare(sizeAddress(), locationAddress());
        }

        private void setKey(byte[] key, int len) {
            if (len > fixedKeyLength) {
                throw new IllegalArgumentException("Invalid key write beyond fixedKeyLength, length: " + len);
            }
            Uns.copyMemory(key, 0, keyAddress(), 0, len);
        }

        private void setExtendedKey(byte[] key, int keyoffset, int len) {
            if (len > slotSize - sizesOffset) {
                throw new IllegalArgumentException("Invalid key write beyond slot with extended key, length: " + len);
            }
            Uns.copyMemory(key, keyoffset, extendedKeyAddress(), 0, len);
        }

        private long sizeAddress() {
            return address + offset + sizesOffset;
        }

        private long locationAddress() {
            return address + offset + locationOffset;
        }

        private long keyAddress() {
            return address + offset + fixedKeyOffset;
        }

        private long extendedKeyAddress() {
            return sizeAddress();
        }
    }
}
