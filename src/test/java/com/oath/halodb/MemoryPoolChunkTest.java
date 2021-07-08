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

    private void createChunk() {
        serializer = ByteArrayEntrySerializer.ofSize(Math.max(fixedEntryLength - 5, 0)); // uses 2 bytes for key size
        chunk = MemoryPoolChunk.create(1, chunkSize, fixedKeyLength, serializer);
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
        int slotSize = MemoryPoolChunk.slotSize(fixedKeyLength, serializer);
        int slots = chunkSize / slotSize;
        final int firstOffset = chunk.getWriteOffset();

        Assert.assertEquals(chunk.remainingSlots(), chunkSize/slotSize);
        Assert.assertEquals(chunk.getWriteOffset(), 0);

        // write to an empty slot.
        byte[] key = Longs.toByteArray(101);
        ByteArrayEntry entry = randomEntry(key.length);
        int nextAddress = MemoryPoolAddress.encode(10, 34343);
        MemoryPoolChunk<ByteArrayEntry>.Slot slot = chunk.allocateSlot();
        slot.fillSlot(key, entry, nextAddress);

        Assert.assertEquals(chunk.getWriteOffset(), firstOffset + slotSize);
        Assert.assertEquals(chunk.remainingSlots(), slots - 1);
        Assert.assertEquals(slot.getKeyLength(), key.length);
        Assert.assertTrue(slot.compareFixedKey(key, key.length));
        Assert.assertTrue(slot.compareEntry(entry));
        Assert.assertEquals(slot.readEntry(), entry);

        int actual = slot.getNextAddress();
        Assert.assertEquals(actual, nextAddress);

        // write to the next empty slot.
        byte[] key2 = randomKey();
        ByteArrayEntry entry2 = randomEntry(key2.length);
        int nextAddress2 = MemoryPoolAddress.encode(0, 4454545);
        MemoryPoolChunk<ByteArrayEntry>.Slot slot2 = chunk.allocateSlot();
        slot2.fillSlot(key2, entry2, nextAddress2);
        Assert.assertEquals(chunk.getWriteOffset(), firstOffset + 2*slotSize);
        Assert.assertEquals(chunk.remainingSlots(), slots - 2);

        Assert.assertEquals(slot2.getKeyLength(), key2.length);
        Assert.assertTrue(slot2.compareFixedKey(key2, key2.length));
        Assert.assertTrue(slot2.compareEntry(entry2));
        Assert.assertEquals(slot2.readEntry(), entry2);

        actual = slot2.getNextAddress();
        Assert.assertEquals(actual, nextAddress2);

        // update an existing slot.
        byte[] key3 = Longs.toByteArray(0x64735981289L);
        ByteArrayEntry entry3 = randomEntry(key3.length);
        int nextAddress3 = MemoryPoolAddress.empty;
        slot.fillSlot(key3, entry3, nextAddress3);

        Assert.assertEquals(slot.getKeyLength(), key3.length);
        Assert.assertTrue(slot.compareFixedKey(key3, key3.length));
        Assert.assertTrue(slot.compareEntry(entry3));
        Assert.assertEquals(slot.readEntry(), entry3);

        // write offset should remain unchanged.
        Assert.assertEquals(chunk.getWriteOffset(), firstOffset + 2*slotSize);
        Assert.assertEquals(chunk.remainingSlots(), slots - 2);

        Hasher hasher = Hasher.create(HashAlgorithm.MURMUR3);
        long keyHash = hasher.hash(key3);
        long entryKeyHash = slot.computeFixedKeyHash(hasher, key3.length);
        Assert.assertEquals(entryKeyHash, keyHash);

        ByteArrayEntry entry4 = randomEntry(key3.length);
        slot.setEntry(entry4);
        Assert.assertEquals(slot.readEntry(), entry4);

        long address = Uns.allocate(fixedKeyLength);
        try {
            slot.copyEntireFixedKey(address);
            Uns.compare(address, key3, 0, key3.length);
        } finally {
            Uns.free(address);
        }
    }

    @Test
    public void testExtendedSlot() {
        createChunk();
        MemoryPoolChunk<ByteArrayEntry>.Slot slot = chunk.allocateSlot();
        byte[] key = HashTableTestUtils.randomBytes(200);
        int next = MemoryPoolAddress.encode(33, 5);

        int writeLen = fixedKeyLength + fixedEntryLength - 7;
        slot.fillOverflowSlot(key, key.length - writeLen, writeLen, next);
        Assert.assertTrue(slot.compareExtendedKey(key, key.length - writeLen, writeLen));

        long address = Uns.allocate(writeLen);
        try {
            slot.copyExtendedKey(address, 0, writeLen);
            Uns.compare(address, key, key.length - writeLen, writeLen);
        } finally {
            Uns.free(address);
        }
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "Invalid request.*")
    public void testWithInvalidSlot() {
        chunkSize = 256;
        fixedKeyLength = 100;
        fixedEntryLength = 100;
        createChunk();
        chunk.slotFor(999);
    }

    @Test
    public void testCompare() {
        chunkSize = 1024;
        fixedKeyLength = 9;
        fixedEntryLength = 15;
        createChunk();

        byte[] key = randomKey();
        ByteArrayEntry entry = randomEntry(key.length);
        MemoryPoolChunk<ByteArrayEntry>.Slot slot = chunk.allocateSlot();
        slot.fillSlot(key, entry, MemoryPoolAddress.empty);

        Assert.assertEquals(slot.getKeyLength(), key.length);
        Assert.assertTrue(slot.compareFixedKey(key, key.length));
        Assert.assertTrue(slot.compareEntry(entry));

        byte[] smallKey = new byte[key.length-1];
        System.arraycopy(key, 0, smallKey, 0, smallKey.length);
        Assert.assertNotEquals(slot.getKeyLength(), smallKey.length);

        key[fixedKeyLength-1] = (byte)~key[fixedKeyLength-1];
        Assert.assertEquals(slot.getKeyLength(), key.length);
        Assert.assertFalse(slot.compareFixedKey(key, key.length));

        entry.bytes[0] = (byte)~entry.bytes[0];
        Assert.assertFalse(slot.compareEntry(entry));
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "Invalid request.*")
    public void testCompareKeyWithException() {
        chunkSize = 1024;
        fixedKeyLength = r.nextInt(100);
        fixedEntryLength = r.nextInt(100);

        createChunk();
        byte[] key = randomKey();
        ByteArrayEntry entry = randomEntry(key.length);
        MemoryPoolChunk<ByteArrayEntry>.Slot slot = chunk.allocateSlot();
        slot.fillSlot(key, entry, MemoryPoolAddress.empty);

        byte[] bigKey = HashTableTestUtils.randomBytes(fixedKeyLength + 1);
        slot.compareFixedKey(bigKey, bigKey.length);
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "Invalid request.*")
    public void testCompareExtendedKeyWithException() {
        chunkSize = 1024;
        fixedKeyLength = r.nextInt(100);
        fixedEntryLength = r.nextInt(100);

        createChunk();
        byte[] key = randomKey();
        MemoryPoolChunk<ByteArrayEntry>.Slot slot = chunk.allocateSlot();
        slot.compareExtendedKey(key, 0, 300);
    }

    @Test
    public void setAndGetNextAddress() {
        chunkSize = 1024;
        fixedKeyLength = r.nextInt(100);
        fixedEntryLength = r.nextInt(100);

        createChunk();

        int slotSize = MemoryPoolChunk.slotSize(fixedKeyLength, serializer);
        int numSlots = chunkSize / slotSize;

        int nextAddress = MemoryPoolAddress.encode((r.nextInt(255) + 1), r.nextInt(numSlots));
        MemoryPoolChunk<ByteArrayEntry>.Slot slot = chunk.slotFor(r.nextInt(numSlots));

        slot.setNextAddress(nextAddress);

        Assert.assertEquals(slot.getNextAddress(), nextAddress);
    }

    @Test
    public void testInvalidSlotSize() {
        MemoryPoolAddress.encode(0, MemoryPoolAddress.MAX_NUMBER_OF_SLOTS);
        try {
            MemoryPoolAddress.encode(0, MemoryPoolAddress.MAX_NUMBER_OF_SLOTS + 1);
            Assert.fail("MemoryPoolAddress should throw when attempting to encode an invalid slot size");
        } catch (IllegalArgumentException expected){
            // nothing
        }
    }

    @Test
    public void testInvalidChunkEncode() {
        MemoryPoolAddress.encode(255, 0);
        try {
            MemoryPoolAddress.encode(256, 0);
            Assert.fail("MemoryPoolAddress should throw when attempting to encode an invalid chunk id");
        } catch (IllegalArgumentException expected){
            // nothing
        }
    }

}
