/*
 * Copyright 2018, Oath Inc
 * Licensed under the terms of the Apache License 2.0. Please refer to accompanying LICENSE file for terms.
 */

package com.oath.halodb;

import com.google.common.primitives.Longs;

import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import java.util.Random;

/**
 * @author Arjun Mannaly
 */
public class MemoryPoolChunkTest {

    private MemoryPoolChunk chunk = null;

    @AfterMethod(alwaysRun = true)
    private void destroyChunk() {
        if (chunk != null) {
            chunk.destroy();
        }
    }

    @Test
    public void testSetAndGetMethods() {
        int chunkSize = 16 * 1024;
        int fixedKeyLength = 12, fixedValueLength = 20;
        int slotSize = MemoryPoolHashEntries.HEADER_SIZE + fixedKeyLength + fixedValueLength;

        chunk = MemoryPoolChunk.create(chunkSize, fixedKeyLength, fixedValueLength);
        int offset = chunk.getWriteOffset();

        Assert.assertEquals(chunk.remaining(), chunkSize);
        Assert.assertEquals(chunk.getWriteOffset(), 0);

        // write to an empty slot.
        byte[] key = Longs.toByteArray(101);
        byte[] value = HashTableTestUtils.randomBytes(fixedValueLength);
        MemoryPoolAddress nextAddress = new MemoryPoolAddress((byte) 10, 34343);
        chunk.fillNextSlot(key, value, nextAddress);

        Assert.assertEquals(chunk.getWriteOffset(), offset + slotSize);
        Assert.assertEquals(chunk.remaining(), chunkSize-slotSize);
        Assert.assertTrue(chunk.compareKey(offset, key));
        Assert.assertTrue(chunk.compareValue(offset, value));

        MemoryPoolAddress actual = chunk.getNextAddress(offset);
        Assert.assertEquals(actual.chunkIndex, nextAddress.chunkIndex);
        Assert.assertEquals(actual.chunkOffset, nextAddress.chunkOffset);

        // write to the next empty slot.
        byte[] key2 = HashTableTestUtils.randomBytes(fixedKeyLength);
        byte[] value2 = HashTableTestUtils.randomBytes(fixedValueLength);
        MemoryPoolAddress nextAddress2 = new MemoryPoolAddress((byte) 0, 4454545);
        chunk.fillNextSlot(key2, value2, nextAddress2);
        Assert.assertEquals(chunk.getWriteOffset(), offset + 2*slotSize);
        Assert.assertEquals(chunk.remaining(), chunkSize-2*slotSize);

        offset += slotSize;
        Assert.assertTrue(chunk.compareKey(offset, key2));
        Assert.assertTrue(chunk.compareValue(offset, value2));

        actual = chunk.getNextAddress(offset);
        Assert.assertEquals(actual.chunkIndex, nextAddress2.chunkIndex);
        Assert.assertEquals(actual.chunkOffset, nextAddress2.chunkOffset);

        // update an existing slot.
        byte[] key3 = Longs.toByteArray(0x64735981289L);
        byte[] value3 = HashTableTestUtils.randomBytes(fixedValueLength);
        MemoryPoolAddress nextAddress3 = new MemoryPoolAddress((byte)-1, -1);
        chunk.fillSlot(0, key3, value3, nextAddress3);

        offset = 0;
        Assert.assertTrue(chunk.compareKey(offset, key3));
        Assert.assertTrue(chunk.compareValue(offset, value3));

        // write offset should remain unchanged.
        Assert.assertEquals(chunk.getWriteOffset(), offset + 2*slotSize);
        Assert.assertEquals(chunk.remaining(), chunkSize-2*slotSize);
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "Invalid offset.*")
    public void testWithInvalidOffset() {
        int chunkSize = 256;
        int fixedKeyLength = 100, fixedValueLength = 100;
        MemoryPoolAddress next = new MemoryPoolAddress((byte)-1, -1);
        chunk = MemoryPoolChunk.create(chunkSize, fixedKeyLength, fixedValueLength);
        chunk.fillSlot(chunkSize - 5, HashTableTestUtils.randomBytes(fixedKeyLength), HashTableTestUtils.randomBytes(fixedValueLength), next);
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "Invalid request. Key length.*")
    public void testWithInvalidKey() {
        int chunkSize = 256;
        int fixedKeyLength = 32, fixedValueLength = 100;
        MemoryPoolAddress next = new MemoryPoolAddress((byte)-1, -1);
        chunk = MemoryPoolChunk.create(chunkSize, fixedKeyLength, fixedValueLength);
        chunk.fillSlot(chunkSize - 5, HashTableTestUtils.randomBytes(fixedKeyLength + 10), HashTableTestUtils.randomBytes(fixedValueLength), next);
    }

    @Test
    public void testCompare() {
        int chunkSize = 1024;
        int fixedKeyLength = 9, fixedValueLength = 15;

        chunk = MemoryPoolChunk.create(chunkSize, fixedKeyLength, fixedValueLength);
        byte[] key = HashTableTestUtils.randomBytes(fixedKeyLength);
        byte[] value = HashTableTestUtils.randomBytes(fixedValueLength);
        int offset = 0;
        chunk.fillSlot(offset, key, value, new MemoryPoolAddress((byte)-1, -1));

        Assert.assertTrue(chunk.compareKey(offset, key));
        Assert.assertTrue(chunk.compareValue(offset, value));

        byte[] smallKey = new byte[key.length-1];
        System.arraycopy(key, 0, smallKey, 0, smallKey.length);
        Assert.assertFalse(chunk.compareKey(offset, smallKey));

        key[fixedKeyLength-1] = (byte)~key[fixedKeyLength-1];
        Assert.assertFalse(chunk.compareKey(offset, key));

        value[0] = (byte)~value[0];
        Assert.assertFalse(chunk.compareValue(offset, value));
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "Invalid request.*")
    public void testCompareKeyWithException() {
        int chunkSize = 1024;
        Random r = new Random();
        int fixedKeyLength = 1 + r.nextInt(100), fixedValueLength = 1 + r.nextInt(100);
        
        chunk = MemoryPoolChunk.create(chunkSize, fixedKeyLength, fixedValueLength);
        byte[] key = HashTableTestUtils.randomBytes(fixedKeyLength);
        byte[] value = HashTableTestUtils.randomBytes(fixedValueLength);
        int offset = 0;
        chunk.fillSlot(offset, key, value, new MemoryPoolAddress((byte)-1, -1));

        byte[] bigKey = HashTableTestUtils.randomBytes(fixedKeyLength + 1);
        chunk.compareKey(offset, bigKey);


    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "Invalid request.*")
    public void testCompareValueWithException() {
        int chunkSize = 1024;
        Random r = new Random();
        int fixedKeyLength = 1 + r.nextInt(100), fixedValueLength = 1 + r.nextInt(100);

        chunk = MemoryPoolChunk.create(chunkSize, fixedKeyLength, fixedValueLength);
        byte[] key = HashTableTestUtils.randomBytes(fixedKeyLength);
        byte[] value = HashTableTestUtils.randomBytes(fixedValueLength);
        int offset = 0;
        chunk.fillSlot(offset, key, value, new MemoryPoolAddress((byte)-1, -1));

        byte[] bigValue = HashTableTestUtils.randomBytes(fixedValueLength + 1);
        chunk.compareValue(offset, bigValue);
    }

    @Test
    public void setAndGetNextAddress() {
        int chunkSize = 1024;
        Random r = new Random();
        int fixedKeyLength = 1 + r.nextInt(100), fixedValueLength = 1 + r.nextInt(100);

        chunk = MemoryPoolChunk.create(chunkSize, fixedKeyLength, fixedValueLength);

        MemoryPoolAddress nextAddress = new MemoryPoolAddress((byte)r.nextInt(Byte.MAX_VALUE), r.nextInt());
        int offset = r.nextInt(chunkSize - fixedKeyLength - fixedValueLength - MemoryPoolHashEntries.HEADER_SIZE);
        chunk.setNextAddress(offset, nextAddress);

        Assert.assertEquals(chunk.getNextAddress(offset), nextAddress);

    }
}
