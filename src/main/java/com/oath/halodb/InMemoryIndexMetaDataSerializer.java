/*
 * Copyright 2018, Oath Inc
 * Licensed under the terms of the Apache License 2.0. Please refer to accompanying LICENSE file for terms.
 */

package com.oath.halodb;

class InMemoryIndexMetaDataSerializer implements HashEntrySerializer<InMemoryIndexMetaData> {

    @Override
    public void serialize(InMemoryIndexMetaData value, long address) {
        value.serialize(address);
    }

    @Override
    public InMemoryIndexMetaData deserialize(long address) {
        return InMemoryIndexMetaData.deserialize(address);
    }

    @Override
    public int fixedSize() {
        return InMemoryIndexMetaData.SERIALIZED_SIZE;
    }

    @Override
    public short readKeySize(long address) {
        return InMemoryIndexMetaData.getKeySize(address);
    }

    @Override
    public boolean compare(InMemoryIndexMetaData entry, long address) {
        return entry.compare(address);
    }
}
