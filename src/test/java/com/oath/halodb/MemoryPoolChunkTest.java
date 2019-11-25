/*
 * Copyright 2018, Oath Inc
 * Licensed under the terms of the Apache License 2.0. Please refer to accompanying LICENSE file for terms.
 */

package com.oath.halodb;

import java.util.Random;

import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.primitives.Longs;

public class MemoryPoolChunkTest {
    private final Random r = new Random();

    private MemoryPoolChunk<ByteArrayEntry> chunk = null;

    private int chunkSize;
    private int fixedKeyLength;
    private int fixedEntryLength;
    private ByteArrayEntrySerializer serializer;

    MemoryPoolAddress nowhere = new MemoryPoolAddress((byte)-1, -1);

    private void createChunk() {
        serializer = ByteArrayEntrySerializer.ofSize(Math.max(fixedEntryLength - 2, 0)); // uses 2 bytes for key size
        chunk = MemoryPoolChunk.create(chunkSize, fixedKeyLength, serializer);
    }

    private byte[] randomKey() {
        return HashTableTestUtils.randomBytes(fixedKeyLength);
    }

    private ByteArrayEntry randomEntry(int keySize) {
        return serializer.randomEntry(keySize);
    }

    @BeforeMethod(alwaysRun = true)
    private void initParams() {
        chunkSize = 1024;
        fixedKeyLength = 12;
        fixedEntryLength = 20;
    }

    @AfterMethod(alwaysRun = true)
    private void destroyChunk() {
        if (chunk != null) {
            chunk.destroy();
        }
    }

    @Test
    public void testSetAndGetMethods() {
        chunkSize = 16 * 1024;

        createChunk();
        int slotSize = MemoryPoolHashEntries.HEADER_SIZE + fixedKeyLength + fixedEntryLength;
        int offset = chunk.getWriteOffset();

        Assert.assertEquals(chunk.remaining(), chunkSize);
        Assert.assertEquals(chunk.getWriteOffset(), 0);

        // write to an empty slot.
        byte[] key = Longs.toByteArray(101);
        ByteArrayEntry entry = randomEntry(key.length);
        MemoryPoolAddress nextAddress = new MemoryPoolAddress((byte) 10, 34343);
        chunk.fillNextSlot(key, entry, nextAddress);

        Assert.assertEquals(chunk.getWriteOffset(), offset + slotSize);
        Assert.assertEquals(chunk.remaining(), chunkSize-slotSize);
        Assert.assertTrue(chunk.compareKey(offset, key));
        Assert.assertTrue(chunk.compareEntry(offset, entry));

        MemoryPoolAddress actual = chunk.getNextAddress(offset);
        Assert.assertEquals(actual.chunkIndex, nextAddress.chunkIndex);
        Assert.assertEquals(actual.chunkOffset, nextAddress.chunkOffset);

        // write to the next empty slot.
        byte[] key2 = randomKey();
        ByteArrayEntry entry2 = randomEntry(key2.length);
        MemoryPoolAddress nextAddress2 = new MemoryPoolAddress((byte) 0, 4454545);
        chunk.fillNextSlot(key2, entry2, nextAddress2);
        Assert.assertEquals(chunk.getWriteOffset(), offset + 2*slotSize);
        Assert.assertEquals(chunk.remaining(), chunkSize-2*slotSize);

        offset += slotSize;
        Assert.assertTrue(chunk.compareKey(offset, key2));
        Assert.assertTrue(chunk.compareEntry(offset, entry2));

        actual = chunk.getNextAddress(offset);
        Assert.assertEquals(actual.chunkIndex, nextAddress2.chunkIndex);
        Assert.assertEquals(actual.chunkOffset, nextAddress2.chunkOffset);

        // update an existing slot.
        byte[] key3 = Longs.toByteArray(0x64735981289L);
        ByteArrayEntry entry3 = randomEntry(key3.length);
        MemoryPoolAddress nextAddress3 = nowhere;
        chunk.fillSlot(0, key3, entry3, nextAddress3);

        offset = 0;
        Assert.assertTrue(chunk.compareKey(offset, key3));
        Assert.assertTrue(chunk.compareEntry(offset, entry3));

        // write offset should remain unchanged.
        Assert.assertEquals(chunk.getWriteOffset(), offset + 2*slotSize);
        Assert.assertEquals(chunk.remaining(), chunkSize-2*slotSize);
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "Invalid offset.*")
    public void testWithInvalidOffset() {
        chunkSize = 256;
        fixedKeyLength = 100;
        fixedEntryLength = 100;
        createChunk();
        byte[] key = randomKey();
        ByteArrayEntry entry = randomEntry(key.length);
        chunk.fillSlot(chunkSize - 5, key, entry, nowhere);
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "Invalid request. Key length.*")
    public void testWithInvalidKey() {
        chunkSize = 256;
        fixedKeyLength = 32;
        fixedEntryLength = 100;
        createChunk();
        byte[] key =  HashTableTestUtils.randomBytes(fixedKeyLength + 10);
        ByteArrayEntry entry = randomEntry(key.length);
        chunk.fillSlot(chunkSize - 5, key, entry, nowhere);
    }

    @Test
    public void testCompare() {
        chunkSize = 1024;
        fixedKeyLength = 9;
        fixedEntryLength = 15;
        createChunk();

        byte[] key = randomKey();
        ByteArrayEntry entry = randomEntry(key.length);
        int offset = 0;
        chunk.fillSlot(offset, key, entry, nowhere);

        Assert.assertTrue(chunk.compareKey(offset, key));
        Assert.assertTrue(chunk.compareEntry(offset, entry));

        byte[] smallKey = new byte[key.length-1];
        System.arraycopy(key, 0, smallKey, 0, smallKey.length);
        Assert.assertFalse(chunk.compareKey(offset, smallKey));

        key[fixedKeyLength-1] = (byte)~key[fixedKeyLength-1];
        Assert.assertFalse(chunk.compareKey(offset, key));

        entry.bytes[0] = (byte)~entry.bytes[0];
        Assert.assertFalse(chunk.compareEntry(offset, entry));
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "Invalid request.*")
    public void testCompareKeyWithException() {
        chunkSize = 1024;
        fixedKeyLength = r.nextInt(100);
        fixedEntryLength = r.nextInt(100);

        createChunk();
        byte[] key = randomKey();
        ByteArrayEntry entry = randomEntry(key.length);
        int offset = 0;
        chunk.fillSlot(offset, key, entry, nowhere);

        byte[] bigKey = HashTableTestUtils.randomBytes(fixedKeyLength + 1);
        chunk.compareKey(offset, bigKey);


    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "Invalid request.*")
    public void testCompareEntryWithException() {
        chunkSize = 1024;
        fixedKeyLength = r.nextInt(100);
        fixedEntryLength = r.nextInt(100);

        createChunk();
        byte[] key = randomKey();
        ByteArrayEntry entry = randomEntry(key.length);
        chunk.compareEntry(chunkSize, entry);
    }

    @Test
    public void setAndGetNextAddress() {
        chunkSize = 1024;
        fixedKeyLength = r.nextInt(100);
        fixedEntryLength = r.nextInt(100);

        createChunk();

        MemoryPoolAddress nextAddress = new MemoryPoolAddress((byte)r.nextInt(Byte.MAX_VALUE), r.nextInt());
        int offset = r.nextInt(chunkSize - fixedKeyLength - fixedEntryLength - MemoryPoolHashEntries.HEADER_SIZE);
        chunk.setNextAddress(offset, nextAddress);

        Assert.assertEquals(chunk.getNextAddress(offset), nextAddress);
    }
}
