/*
 * Copyright 2018, Oath Inc
 * Licensed under the terms of the Apache License 2.0. Please refer to accompanying LICENSE file for terms.
 */

// This code is a derivative work heavily modified from the OHC project. See NOTICE file for copyright and license.

package com.oath.halodb;

/**
 * Serialize and deserialize cached data
 */
interface HashEntrySerializer<E extends HashEntry> {

    /** The entry must contain the key size **/
    short readKeySize(long address);

    void serialize(E entry, long address);

    E deserialize(long address);

    /** The fixed size of the hash table entry. **/
    int fixedSize();

    boolean compare(E entry, long entryAddress);
}

