/*
 * Copyright 2018, Oath Inc
 * Licensed under the terms of the Apache License 2.0. Please refer to accompanying LICENSE file for terms.
 */

package com.oath.halodb;

class InMemoryIndexMetaDataSerializer extends HashEntrySerializer<InMemoryIndexMetaData> {

    @Override
    final InMemoryIndexMetaData deserialize(long sizeAddress, long locationAddress) {
        int firstWord = Uns.getInt(sizeAddress, 0);
        byte nextByte = Uns.getByte(sizeAddress, 4);
        short keySize = HashEntry.extractKeySize(firstWord);
        int valueSize = HashEntry.extractValueSize(firstWord, nextByte);
        int fileId = HashEntryLocation.readFileId(locationAddress);
        int valueOffset = HashEntryLocation.readValueOffset(locationAddress);
        long sequenceNumber = HashEntryLocation.readSequenceNumber(locationAddress);
        return new InMemoryIndexMetaData(fileId, valueOffset, valueSize, sequenceNumber, keySize);
    }

    @Override
    final int locationSize() {
        return HashEntryLocation.ENTRY_LOCATION_SIZE;
    }

    @Override
    boolean validSize(InMemoryIndexMetaData entry) {
        return true;
    }
}
