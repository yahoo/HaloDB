/*
 * Licensed under the terms of the Apache License 2.0. Please refer to accompanying LICENSE file for terms.
 */

package com.oath.halodb;

/**
 * Metadata stored in the in-memory index for referencing the location of a record in a file.
 */
interface HashEntryLocation {

    /*
     * file id            - 4 bytes
     * value offset       - 4 bytes
     * sequence number    - 8 bytes
     */
    int ENTRY_LOCATION_SIZE = 4 + 4 + 8;

    static void serializeLocation(long address, int fileId, int valueOffset, long sequenceNumber) {
        Uns.putInt(address, 0, fileId);
        Uns.putInt(address, 4, valueOffset);
        Uns.putLong(address, 8, sequenceNumber);
    }

    static boolean compareLocation(long address, int fileId, int valueOffset, long sequenceNumber) {
        return fileId == Uns.getInt(address, 0)
            && valueOffset == Uns.getInt(address, 4)
            && sequenceNumber == Uns.getLong(address, 8);
    }

    static int readFileId(long address) {
        return Uns.getInt(address, 0);
    }

    static int readValueOffset(long address) {
        return Uns.getInt(address, 4);
    }

    static long readSequenceNumber(long address) {
        return Uns.getLong(address, 8);
    }

    int getFileId();

    int getValueOffset();

    long getSequenceNumber();
}
