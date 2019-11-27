/*
 * Copyright 2018, Oath Inc
 * Licensed under the terms of the Apache License 2.0. Please refer to accompanying LICENSE file for terms.
 */

package com.oath.halodb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import com.google.common.collect.Lists;

public class SegmentWithMemoryPoolTest {

    Hasher hasher = Hasher.create(HashAlgorithm.MURMUR3);

    int fixedKeySize = 8;
    int noOfEntries = 100;
    int noOfChunks = 2;
    ByteArrayEntrySerializer serializer = ByteArrayEntrySerializer.ofSize(16);
    int fixedSlotSize = MemoryPoolHashEntries.HEADER_SIZE + fixedKeySize + serializer.fixedSize();

    SegmentWithMemoryPool<ByteArrayEntry> segment = null;

    private OffHeapHashTableBuilder<ByteArrayEntry> builder() {
        return OffHeapHashTableBuilder
            .newBuilder(serializer)
            .fixedKeySize(fixedKeySize)
            // chunkSize set such that noOfEntries/2 can set filled in one.
            .memoryPoolChunkSize(noOfEntries/noOfChunks * fixedSlotSize);
    }

    @AfterMethod(alwaysRun = true)
    public void releaseSegment() {
        if (segment != null) {
            segment.release();
        }
    }

    @Test
    public void testChunkAllocations() {
        segment = new SegmentWithMemoryPool<>(builder());

        addEntriesToSegment(segment, noOfEntries);

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
        MemoryPoolAddress emptyList = new MemoryPoolAddress((byte) -1, -1);

        segment = new SegmentWithMemoryPool<>(builder());

        //Add noOfEntries to the segment. This should require chunks.
        List<Record> records = addEntriesToSegment(segment, noOfEntries);

        // each chunk can hold noOfEntries/2 hence two chunks must be allocated.
        Assert.assertEquals(segment.numberOfChunks(), noOfChunks);
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
        records = addEntriesToSegment(segment, noOfEntries);

        Assert.assertEquals(segment.numberOfChunks(), noOfChunks);
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
        addEntriesToSegment(segment, elementsRemoved);

        Assert.assertEquals(segment.numberOfChunks(), noOfChunks);
        Assert.assertEquals(segment.size(), noOfEntries);
        Assert.assertEquals(segment.freeListSize(), 0);
        Assert.assertEquals(segment.getFreeListHead(), emptyList);
    }

    @Test(expectedExceptions = OutOfMemoryError.class, expectedExceptionsMessageRegExp = "Each segment can have at most 128 chunks.")
    public void testOutOfMemoryException() {
        // Each segment can have only Byte.MAX_VALUE chunks.
        // we add more that that.
        noOfEntries = Byte.MAX_VALUE * 2;

        segment = new SegmentWithMemoryPool<>(builder().memoryPoolChunkSize(fixedSlotSize));
        addEntriesToSegment(segment, noOfEntries);
    }

    @Test
    public void testReplace() {
        noOfEntries = 1000;
        noOfChunks = 10;

        segment = new SegmentWithMemoryPool<>(builder());

        Map<KeyBuffer, ByteArrayEntry> map = new HashMap<>();
        for (int i = 0; i < noOfEntries; i++) {
            byte[] key = HashTableTestUtils.randomBytes(fixedKeySize);
            KeyBuffer k = new KeyBuffer(key);
            k.finish(hasher);
            ByteArrayEntry value = serializer.randomEntry(key.length);
            map.put(k, value);
            segment.putEntry(key, value, k.hash(), true, null);
        }

        Assert.assertEquals(segment.numberOfChunks(), noOfChunks);
        Assert.assertEquals(segment.size(), noOfEntries);
        Assert.assertEquals(segment.putAddCount(), noOfEntries);
        Assert.assertEquals(segment.freeListSize(), 0);

        map.forEach((k, v) -> {
            ByteArrayEntry newEntry = serializer.randomEntry(k.size());
            Assert.assertTrue(segment.putEntry(k.buffer, newEntry, k.hash(), false, v));
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
        noOfEntries = 100_000;
        noOfChunks = 10;

        OffHeapHashTableBuilder<ByteArrayEntry> builder = OffHeapHashTableBuilder
            .newBuilder(serializer)
            .fixedKeySize(fixedKeySize)
            // chunkSize set such that noOfEntries/2 can set filled in one.
            .memoryPoolChunkSize(noOfEntries/noOfChunks * fixedSlotSize)
            .hashTableSize(noOfEntries/8) // size of table less than number of entries, this will trigger a rehash.
            .loadFactor(1);

        segment = new SegmentWithMemoryPool<>(builder);
        List<Record> records = addEntriesToSegment(segment, noOfEntries);

        Assert.assertEquals(segment.size(), noOfEntries);
        Assert.assertEquals(segment.rehashes(), 3);
        Assert.assertEquals(segment.putAddCount(), noOfEntries);

        records.forEach(r -> Assert.assertEquals(segment.getEntry(r.keyBuffer), r.entry));
    }

    private List<Record> addEntriesToSegment(SegmentWithMemoryPool<ByteArrayEntry> segment, int noOfEntries) {
        List<Record> records = new ArrayList<>();
        for (int i = 0; i < noOfEntries; i++) {
            byte[] key = HashTableTestUtils.randomBytes(fixedKeySize);
            KeyBuffer k = new KeyBuffer(key);
            k.finish(hasher);
            ByteArrayEntry value = serializer.randomEntry(key.length);
            records.add(new Record(k, value));
            segment.putEntry(key, value, k.hash(), true, null);
        }
        return records;
    }

    private static class Record {
        final KeyBuffer keyBuffer;
        final ByteArrayEntry entry;

        public Record(KeyBuffer keyBuffer, ByteArrayEntry entry) {
            this.keyBuffer = keyBuffer;
            this.entry = entry;
        }
    }
}
