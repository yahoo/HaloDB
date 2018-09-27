/*
 * Copyright 2018, Oath Inc
 * Licensed under the terms of the Apache License 2.0. Please refer to accompanying LICENSE file for terms.
 */

package com.oath.halodb;

import java.nio.ByteBuffer;

/**
 * @author Arjun Mannaly
 */
class InMemoryIndexMetaDataSerializer implements HashTableValueSerializer<InMemoryIndexMetaData> {

    public void serialize(InMemoryIndexMetaData recordMetaData, ByteBuffer byteBuffer) {
        recordMetaData.serialize(byteBuffer);
        byteBuffer.flip();
    }

    public InMemoryIndexMetaData deserialize(ByteBuffer byteBuffer) {
        return InMemoryIndexMetaData.deserialize(byteBuffer);
    }

    public int serializedSize(InMemoryIndexMetaData recordMetaData) {
        return InMemoryIndexMetaData.SERIALIZED_SIZE;
    }
}
