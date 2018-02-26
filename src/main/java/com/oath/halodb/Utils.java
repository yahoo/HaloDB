/*
 * Copyright 2018, Oath Inc
 * Licensed under the terms of the Apache License 2.0. Please refer to accompanying LICENSE file for terms.
 */

package com.oath.halodb;

/**
 * @author Arjun Mannaly
 */
class Utils {
    static long roundUpToPowerOf2(long number) {
        return (number > 1) ? Long.highestOneBit((number - 1) << 1) : 1;
    }

    static int getValueOffset(int recordOffset, byte[] key) {
        return recordOffset + Record.Header.HEADER_SIZE + key.length;
    }

    //TODO: probably belongs to Record.
    static int getRecordSize(int keySize, int valueSize) {
        return keySize + valueSize + Record.Header.HEADER_SIZE;
    }

    static int getValueSize(int recordSize, byte[] key) {
        return recordSize - Record.Header.HEADER_SIZE - key.length;
    }

    static RecordMetaDataForCache getMetaData(IndexFileEntry entry, int fileId) {
        return new RecordMetaDataForCache(fileId, Utils.getValueOffset(entry.getRecordOffset(), entry.getKey()), Utils.getValueSize(entry.getRecordSize(), entry.getKey()), entry.getSequenceNumber());
    }
}
