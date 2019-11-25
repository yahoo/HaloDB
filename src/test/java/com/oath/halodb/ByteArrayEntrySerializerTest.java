/*
 * Copyright 2018, Oath Inc
 * Licensed under the terms of the Apache License 2.0. Please refer to accompanying LICENSE file for terms.
 */

package com.oath.halodb;

import org.testng.Assert;
import org.testng.annotations.Test;

public class ByteArrayEntrySerializerTest implements HashEntrySerializerTest {

    ByteArrayEntrySerializer serializer = ByteArrayEntrySerializer.ofSize(4);
    short ksize = 1111;
    byte[] data = new byte[] { 0, 1, 2, 3 };
    byte[] data2 = new byte[] { 3, 2, 1, 0 };
    byte[] tooLarge = new byte[] { 0, 1, 2, 3, 4 };

    @Test
    public void testSerializeDeserialize() {

        ByteArrayEntry entry = serializer.createEntry(ksize, data);
        Assert.assertEquals(entry.getKeySize(), ksize);
        Assert.assertEquals(data, entry.bytes);

        ByteArrayEntry entry2 = serializer.createEntry(ksize, data2);
        Assert.assertNotEquals(entry2, entry);
        ByteArrayEntry entry3 = serializer.createEntry(1, data);
        Assert.assertNotEquals(entry3, entry);
        Assert.assertEquals(entry, entry);

        Assert.assertFalse(entry.equals(new Object()));
        Assert.assertFalse(entry.equals(null));

        ByteArrayEntry readEntry = testSerDe(entry, serializer, (e1, e2) -> e2.equals(e1));
        Assert.assertEquals(readEntry.hashCode(), entry.hashCode());
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testInvalidSize() {
        serializer.createEntry(ksize, tooLarge);
    }

    @Test(expectedExceptions = RuntimeException.class, expectedExceptionsMessageRegExp = "boom")
    public void testSerializationFailure() {
        ByteArrayEntrySerializer serializer = ByteArrayEntrySerializer.ofSizeFailSerialize(4);
        serializer.serialize(serializer.randomEntry(0), 0);
    }

}
