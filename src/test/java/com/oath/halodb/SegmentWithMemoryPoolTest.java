/*
 * Copyright 2018, Oath Inc
 * Licensed under the terms of the Apache License 2.0. Please refer to accompanying LICENSE file for terms.
 */

package com.oath.halodb;

import com.google.common.collect.Lists;

import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * @author Arjun Mannaly
 */
public class SegmentWithMemoryPoolTest {

    @Test
    public void testChunkAllocations() {

        int fixedKeySize = 8;
        int fixedValueSize = 18;
        int noOfEntries = 100;
        int noOfChunks = 2;
        int fixedSlotSize = MemoryPoolHashEntries.HEADER_SIZE + fixedKeySize + fixedValueSize;

        OffHeapHashTableBuilder<byte[]> builder = OffHeapHashTableBuilder
            .<byte[]>newBuilder()
            .fixedKeySize(fixedKeySize)
            .fixedValueSize(fixedValueSize)
            // chunkSize set such that noOfEntries/2 can set filled in one. 
            .memoryPoolChunkSize(noOfEntries/noOfChunks * fixedSlotSize)
            .valueSerializer(HashTableTestUtils.byteArraySerializer);

        SegmentWithMemoryPool<byte[]> segment = new SegmentWithMemoryPool<>(builder);

        addEntriesToSegment(segment, Hasher.create(HashAlgorithm.MURMUR3), noOfEntries, fixedKeySize, fixedValueSize);

        // each chunk can hold noOfEntries/2 hence two chunks must be allocated. 
        Assert.assertEquals(segment.numberOfChunks(), 2);
        Assert.assertEquals(segment.numberOfSlots(), noOfEntries);
        Assert.assertEquals(segment.size(), noOfEntries);
        Assert.assertEquals(segment.putAddCount(), noOfEntries);
        Assert.assertEquals(segment.freeListSize(), 0);

        // All slots in chunk should be written to. 
        for (int i = 0; i < segment.numberOfChunks(); i++) {
            Assert.assertEquals(segment.getChunkWriteOffset(i), noOfEntries/noOfChunks * fixedSlotSize);
        }
    }

    @Test
    public void testFreeList() {
        int fixedKeySize = 8;
        int fixedValueSize = 18;
        int noOfEntries = 100;
        int chunkCount = 2;
        int fixedSlotSize = MemoryPoolHashEntries.HEADER_SIZE + fixedKeySize + fixedValueSize;
        MemoryPoolAddress emptyList = new MemoryPoolAddress((byte) -1, -1);

        OffHeapHashTableBuilder<byte[]> builder = OffHeapHashTableBuilder
            .<byte[]>newBuilder()
            .fixedKeySize(fixedKeySize)
            .fixedValueSize(fixedValueSize)
            // chunkSize set such that noOfEntries/2 can set filled in one.
            .memoryPoolChunkSize(noOfEntries / chunkCount * fixedSlotSize)
            .valueSerializer(HashTableTestUtils.byteArraySerializer);

        SegmentWithMemoryPool<byte[]> segment = new SegmentWithMemoryPool<>(builder);

        //Add noOfEntries to the segment. This should require chunks.
        Hasher hasher = Hasher.create(HashAlgorithm.MURMUR3);
        List<Record> records = addEntriesToSegment(segment, hasher, noOfEntries, fixedKeySize, fixedValueSize);

        // each chunk can hold noOfEntries/2 hence two chunks must be allocated.
        Assert.assertEquals(segment.numberOfChunks(), chunkCount);
        Assert.assertEquals(segment.size(), noOfEntries);
        Assert.assertEquals(segment.putAddCount(), noOfEntries);
        Assert.assertEquals(segment.freeListSize(), 0);
        Assert.assertEquals(segment.getFreeListHead(), emptyList);

        // remove all entries from the segment
        // all slots should now be part of the free list. 
        Lists.reverse(records).forEach(k -> segment.removeEntry(k.keyBuffer));

        Assert.assertEquals(segment.freeListSize(), noOfEntries);
        Assert.assertNotEquals(segment.getFreeListHead(), emptyList);
        Assert.assertEquals(segment.removeCount(), noOfEntries);
        Assert.assertEquals(segment.size(), 0);

        // Add noOfEntries to the segment.
        // All entries must be added to slots part of the freelist. 
        records = addEntriesToSegment(segment, hasher, noOfEntries, fixedKeySize, fixedValueSize);

        Assert.assertEquals(segment.numberOfChunks(), chunkCount);
        Assert.assertEquals(segment.size(), noOfEntries);
        Assert.assertEquals(segment.freeListSize(), 0);

        // after all slots in the free list are used head should point to
        // an empty list. 
        Assert.assertEquals(segment.getFreeListHead(), emptyList);

        // remove only some of the elements.
        Random r = new Random();
        int elementsRemoved = 0;
        for (int i = 0; i < noOfEntries/3; i++) {
            if(segment.removeEntry(records.get(r.nextInt(records.size())).keyBuffer))
                elementsRemoved++;
        }

        Assert.assertEquals(segment.freeListSize(), elementsRemoved);
        Assert.assertNotEquals(segment.getFreeListHead(), emptyList);
        Assert.assertEquals(segment.size(), noOfEntries-elementsRemoved);

        // add removed elements back.
        addEntriesToSegment(segment, hasher, elementsRemoved, fixedKeySize, fixedValueSize);

        Assert.assertEquals(segment.numberOfChunks(), chunkCount);
        Assert.assertEquals(segment.size(), noOfEntries);
        Assert.assertEquals(segment.freeListSize(), 0);
        Assert.assertEquals(segment.getFreeListHead(), emptyList);
    }

    @Test(expectedExceptions = OutOfMemoryError.class, expectedExceptionsMessageRegExp = "Each segment can have at most 128 chunks.")
    public void testOutOfMemoryException() {
        int fixedKeySize = 8;
        int fixedValueSize = 18;
        int fixedSlotSize = MemoryPoolHashEntries.HEADER_SIZE + fixedKeySize + fixedValueSize;

        // Each segment can have only Byte.MAX_VALUE chunks.
        // we add more that that.
        int noOfEntries = Byte.MAX_VALUE * 2;

        OffHeapHashTableBuilder<byte[]> builder = OffHeapHashTableBuilder
            .<byte[]>newBuilder()
            .fixedKeySize(fixedKeySize)
            .fixedValueSize(fixedValueSize)
            // chunkSize set so that each can contain only one entry.
            .memoryPoolChunkSize(fixedSlotSize)
            .valueSerializer(HashTableTestUtils.byteArraySerializer);

        SegmentWithMemoryPool<byte[]> segment = new SegmentWithMemoryPool<>(builder);
        addEntriesToSegment(segment, Hasher.create(HashAlgorithm.MURMUR3), noOfEntries, fixedKeySize, fixedValueSize);
    }

    @Test
    public void testReplace() {

        int fixedKeySize = 8;
        int fixedValueSize = 18;
        int noOfEntries = 1000;
        int noOfChunks = 10;
        int fixedSlotSize = MemoryPoolHashEntries.HEADER_SIZE + fixedKeySize + fixedValueSize;

        OffHeapHashTableBuilder<byte[]> builder = OffHeapHashTableBuilder
            .<byte[]>newBuilder()
            .fixedKeySize(fixedKeySize)
            .fixedValueSize(fixedValueSize)
            // chunkSize set such that noOfEntries/2 can set filled in one.
            .memoryPoolChunkSize(noOfEntries/noOfChunks * fixedSlotSize)
            .valueSerializer(HashTableTestUtils.byteArraySerializer);

        SegmentWithMemoryPool<byte[]> segment = new SegmentWithMemoryPool<>(builder);

        Hasher hasher = Hasher.create(HashAlgorithm.MURMUR3);
        Map<KeyBuffer, byte[]> map = new HashMap<>();
        for (int i = 0; i < noOfEntries; i++) {
            byte[] key = HashTableTestUtils.randomBytes(fixedKeySize);
            KeyBuffer k = new KeyBuffer(key);
            k.finish(hasher);
            byte[] value = HashTableTestUtils.randomBytes(fixedValueSize);
            map.put(k, value);
            segment.putEntry(key, value, k.hash(), true, null);
        }

        Assert.assertEquals(segment.numberOfChunks(), noOfChunks);
        Assert.assertEquals(segment.size(), noOfEntries);
        Assert.assertEquals(segment.putAddCount(), noOfEntries);
        Assert.assertEquals(segment.freeListSize(), 0);

        map.forEach((k, v) -> {
            Assert.assertTrue(segment.putEntry(k.buffer, HashTableTestUtils.randomBytes(fixedValueSize), k.hash(), false, v));
        });

        // we have replaced all values. no new chunks should
        // have been allocated.
        Assert.assertEquals(segment.numberOfChunks(), noOfChunks);
        Assert.assertEquals(segment.size(), noOfEntries);
        Assert.assertEquals(segment.putAddCount(), noOfEntries);
        Assert.assertEquals(segment.freeListSize(), 0);
        Assert.assertEquals(segment.putReplaceCount(), noOfEntries);

        // All slots in chunk should be written to.
        for (int i = 0; i < segment.numberOfChunks(); i++) {
            Assert.assertEquals(segment.getChunkWriteOffset(i), noOfEntries/noOfChunks * fixedSlotSize);
        }
    }

    @Test
    public void testRehash() {

        int fixedKeySize = 8;
        int fixedValueSize = 18;
        int noOfEntries = 100_000;
        int noOfChunks = 10;
        int fixedSlotSize = MemoryPoolHashEntries.HEADER_SIZE + fixedKeySize + fixedValueSize;

        OffHeapHashTableBuilder<byte[]> builder = OffHeapHashTableBuilder
            .<byte[]>newBuilder()
            .fixedKeySize(fixedKeySize)
            .fixedValueSize(fixedValueSize)
            .memoryPoolChunkSize(noOfEntries/noOfChunks * fixedSlotSize)
            .hashTableSize(noOfEntries/8) // size of table less than number of entries, this will trigger a rehash.
            .loadFactor(1)
            .valueSerializer(HashTableTestUtils.byteArraySerializer);

        SegmentWithMemoryPool<byte[]> segment = new SegmentWithMemoryPool<>(builder);
        Hasher hasher = Hasher.create(HashAlgorithm.MURMUR3);
        List<Record> records = addEntriesToSegment(segment, hasher, noOfEntries, fixedKeySize, fixedValueSize);

        Assert.assertEquals(segment.size(), noOfEntries);
        Assert.assertEquals(segment.rehashes(), 3);
        Assert.assertEquals(segment.putAddCount(), noOfEntries);

        records.forEach(r -> Assert.assertEquals(segment.getEntry(r.keyBuffer), r.value));
    }



    private List<Record> addEntriesToSegment(SegmentWithMemoryPool<byte[]> segment, Hasher hasher, int noOfEntries, int fixedKeySize, int fixedValueSize) {
        List<Record> records = new ArrayList<>();
        for (int i = 0; i < noOfEntries; i++) {
            byte[] key = HashTableTestUtils.randomBytes(fixedKeySize);
            KeyBuffer k = new KeyBuffer(key);
            k.finish(hasher);
            byte[] value = HashTableTestUtils.randomBytes(fixedValueSize);
            records.add(new Record(k, value));
            segment.putEntry(key, value, k.hash(), true, null);
        }

        return records;
    }

    private static class Record {
        final KeyBuffer keyBuffer;
        final byte[] value;

        public Record(KeyBuffer keyBuffer, byte[] value) {
            this.keyBuffer = keyBuffer;
            this.value = value;
        }
    }
}
