/*
 * Copyright 2018, Oath Inc
 * Licensed under the terms of the Apache License 2.0. Please refer to accompanying LICENSE file for terms.
 */

// This code is a derivative work heavily modified from the OHC project. See NOTICE file for copyright and license.

package com.oath.halodb;

/**
 * Serialize and deserialize cache entry data.
 *
 * The key and value size data are stored independently from
 * the location payload.  Different Segment implementations may
 * store each of these at different relative places in their
 * hash slot.
 */
abstract class HashEntrySerializer<E extends HashEntry> {

    /** read the key size from the given address **/
    final short readKeySize(long sizeAddress) {
        return HashEntry.readKeySize(sizeAddress);
    }

    /** the serialized size of the key and value length **/
    final int sizesSize() {
        return HashEntry.ENTRY_SIZES_SIZE;
    }

    /** the total size of the entry, including sizes and location data **/
    final int entrySize() {
        return sizesSize() + locationSize();
    }

    /** read the entry from memory, from the provided sizeAddress and locationAddress **/
    abstract E deserialize(long sizeAddress, long locationAddress);

    /** the size of the location data **/
    abstract int locationSize();

    final void validateSize(E entry) {
        if (!validSize(entry)) {
            throw new IllegalArgumentException("value size incompatible with fixed value size " + entrySize());
        }
    }

    /** return true if the entry serializes to entrySize bytes **/
    abstract boolean validSize(E entry);
}
