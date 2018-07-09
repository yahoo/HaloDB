/*
 * Copyright 2018, Oath Inc
 * Licensed under the terms of the Apache License 2.0. Please refer to accompanying LICENSE file for terms.
 */

package com.oath.halodb;

/**
 * @author Arjun Mannaly
 */
class MemoryPoolHashEntries {

    /*
     * chunk index - 1 byte.
     * chunk offset - 4 byte.
     * key length - 1 byte.
     */
    static final int HEADER_SIZE = 1 + 4 + 1;

    static final int ENTRY_OFF_NEXT_CHUNK_INDEX = 0;
    static final int ENTRY_OFF_NEXT_CHUNK_OFFSET = 1;

    // offset of key length (1 bytes, byte)
    static final int ENTRY_OFF_KEY_LENGTH = 5;

    // offset of data in first block
    static final int ENTRY_OFF_DATA = 6;

}
