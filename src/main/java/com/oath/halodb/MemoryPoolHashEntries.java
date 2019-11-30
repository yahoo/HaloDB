/*
 * Copyright 2018, Oath Inc
 * Licensed under the terms of the Apache License 2.0. Please refer to accompanying LICENSE file for terms.
 */

package com.oath.halodb;

class MemoryPoolHashEntries {

    /*
     * chunk index - 1 byte.
     * chunk offset - 4 byte.
     */
    static final int HEADER_SIZE = 1 + 4;

    static final int ENTRY_OFF_NEXT_CHUNK_INDEX = 0;
    static final int ENTRY_OFF_NEXT_CHUNK_OFFSET = 1;

    public static int slotSize(int fixedKeySize, HashEntrySerializer<?> serializer) {
        return HEADER_SIZE + fixedKeySize + serializer.entrySize();
    }
}
