/*
 * Licensed under the terms of the Apache License 2.0. Please refer to accompanying LICENSE file for terms.
 */

package com.oath.halodb;

/**
 * The key size and value size stored in the in-memory index for each key.
 *
 * The serialization of these values is shared across all entry types.
 * Different segment implementations may store the key and value sizes
 * in different locations relative to the location information.
 */
abstract class HashEntry {
    /*
     * key and value size - 5 bytes, 11 bits for key size and 29 bits for value size
     */
    static final int ENTRY_SIZES_SIZE = 5;

    private final short keySize;
    private final int valueSize;

    HashEntry(int keySize, int valueSize) {
        this.keySize = Utils.validateKeySize(keySize);
        this.valueSize = Utils.validateValueSize(valueSize);
    }

    static short readKeySize(long address) {
        return extractKeySize(Uns.getInt(address, 0));
    }

    static short extractKeySize(int firstWord) {
        return (short) (firstWord & 0b0111_1111_1111);
    }

    static int extractValueSize(int firstWord, byte nextByte) {
        return (firstWord >>> 11) | nextByte << 21;
    }

    static boolean compareSizes(long address, short keySize, int valueSize) {
        int firstWord = Uns.getInt(address, 0);
        return keySize == extractKeySize(firstWord)
            && valueSize == extractValueSize(firstWord, Uns.getByte(address, 4));
    }

    static void serializeSizes(long address, short keySize, int valueSize) {
        Uns.putInt(address, 0, keySize | valueSize << 11);
        Uns.putByte(address, 4, (byte) (valueSize >>> 21));
    }

    final short getKeySize() {
        return keySize;
    }

    final int getValueSize() {
        return valueSize;
    }

    final boolean compareSizes(long address) {
        return compareSizes(address, getKeySize(), getValueSize());
    }

    final void serializeSizes(long address) {
        serializeSizes(address, getKeySize(), getValueSize());
    }

    final boolean compare(long sizeAddress, long locationAddress) {
        return compareLocation(locationAddress) && compareSizes(sizeAddress);
    }

    /** write the location data to memory at the given address **/
    abstract void serializeLocation(long locationAddress);

    /** return true if this entry's location data matches the data at the given address **/
    abstract boolean compareLocation(long locationAddress);
}


