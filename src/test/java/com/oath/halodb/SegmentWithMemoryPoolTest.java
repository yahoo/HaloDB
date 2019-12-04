/*
 * Copyright 2018, Oath Inc
 * Licensed under the terms of the Apache License 2.0. Please refer to accompanying LICENSE file for terms.
 */

package com.oath.halodb;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.stream.Stream;

import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.collect.Lists;

public class SegmentWithMemoryPoolTest {

    Hasher hasher = Hasher.create(HashAlgorithm.MURMUR3);
    ByteArrayEntrySerializer serializer = ByteArrayEntrySerializer.ofSize(16);

    int fixedKeySize;
    int noOfEntries;
    int noOfChunks;
    int fixedSlotSize;

    SegmentWithMemoryPool<ByteArrayEntry> segment = null;

    private OffHeapHashTableBuilder<ByteArrayEntry> builder() {
        return OffHeapHashTableBuilder
            .newBuilder(serializer)
            .fixedKeySize(fixedKeySize)
            // chunkSize set such that noOfEntries/2 can set filled in one.
            .memoryPoolChunkSize(noOfEntries/noOfChunks * fixedSlotSize);
    }

    @BeforeMethod(alwaysRun = true)
    public void initialize() {
        fixedKeySize = 8;
        noOfEntries = 100;
        noOfChunks = 2;
        fixedSlotSize = MemoryPoolHashEntries.slotSize(fixedKeySize, serializer);
    }

    @AfterMethod(alwaysRun = true)
    public void releaseSegment() {
        if (segment != null) {
            segment.release();
        }
    }

    @Test
    public void testTinyKeys() {
        segment = new SegmentWithMemoryPool<>(builder());
        Record rec = createRecord(0);
        validateBasicPutGet(rec, 1);
        Record rec2 = createRecord(1);
        validateBasicPutGet(rec2, 1);
    }

    @Test
    public void testSmallKeys() {
        segment = new SegmentWithMemoryPool<>(builder());
        Record rec = createRecord(fixedKeySize);
        validateBasicPutGet(rec, 1);
        Record rec2 = createRecord(fixedKeySize -1);
        validateBasicPutGet(rec2, 1);
    }

    @Test
    public void testLargeKeys() {
        segment = new SegmentWithMemoryPool<>(builder());
        Record rec = createRecord(fixedKeySize + 1);
        validateBasicPutGet(rec, 2);
        Record rec2 = createRecord(fixedKeySize + 15);
        validateBasicPutGet(rec2, 2);
        Record rec3 = createRecord(fixedKeySize + fixedKeySize + serializer.entrySize() - 1);
        validateBasicPutGet(rec3, 2);
        Record rec4 = createRecord(fixedKeySize + fixedKeySize + serializer.entrySize());
        validateBasicPutGet(rec4, 2);
    }

    @Test
    public void testHugeKeys() {
        segment = new SegmentWithMemoryPool<>(builder());
        int fillsFourSlots = (fixedKeySize * 4) + (3 * serializer.entrySize());
        int barelyUsesFiveSlots = fillsFourSlots + 1;
        int nearlyUsesFourSlots = fillsFourSlots - 1;
        Record rec = createRecord(fillsFourSlots);
        validateBasicPutGet(rec, 4);
        rec = createRecord(barelyUsesFiveSlots);
        validateBasicPutGet(rec, 5);
        rec = createRecord(nearlyUsesFourSlots);
        validateBasicPutGet(rec, 4);

        Record max = createRecord(2047);
        int expectedSlotsUsed = 2 + ((2047 - fixedKeySize - 1) / (fixedKeySize + serializer.entrySize()));
        validateBasicPutGet(max, expectedSlotsUsed);
    }

    private void validateBasicPutGet(Record rec, int expectedSlotsPerEntry) {
        KeyBuffer key = rec.keyBuffer;
        ByteArrayEntry entry = rec.entry;

        long initialSize = segment.size();
        MemoryPoolAddress initialFreeListHead = segment.getFreeListHead();
        long initialFreeListSize = segment.freeListSize();
        long initialPutAddCount = segment.putAddCount();
        long initialPutReplaceCount = segment.putReplaceCount();
        long initialRemoveCount = segment.removeCount();
        long initialHitCount = segment.hitCount();
        long initialMissCount = segment.missCount();

        // put when not present, but old value exists and doesn't match, so this should not add
        Assert.assertFalse(segment.putEntry(key, entry, false, entry));
        Assert.assertFalse(segment.containsEntry(key));
        Assert.assertNull(segment.getEntry(key));
        Assert.assertEquals(segment.size(), initialSize);
        Assert.assertEquals(segment.freeListSize(), initialFreeListSize);
        Assert.assertEquals(segment.putAddCount(), initialPutAddCount);
        Assert.assertEquals(segment.putReplaceCount(), initialPutReplaceCount);
        Assert.assertEquals(segment.getFreeListHead(), initialFreeListHead);
        Assert.assertEquals(segment.removeCount(), initialRemoveCount);
        Assert.assertEquals(segment.hitCount(), initialHitCount);
        Assert.assertEquals(segment.missCount(), initialMissCount + 2);

        // put with putIfAbsent = false should add -- it only disables overwriting existing
        Assert.assertTrue(segment.putEntry(key, entry, false, null));
        Assert.assertTrue(segment.containsEntry(key));
        Assert.assertEquals(segment.getEntry(key), entry);
        Assert.assertEquals(segment.size(), initialSize + 1);
        Assert.assertEquals(segment.putAddCount(), initialPutAddCount + 1);
        Assert.assertEquals(segment.putReplaceCount(), initialPutReplaceCount);
        Assert.assertEquals(segment.freeListSize(), Math.max(initialFreeListSize - expectedSlotsPerEntry, 0));
        if(initialFreeListHead.isEmpty()) {
            Assert.assertEquals(segment.freeListSize(), initialFreeListSize);
        } else {
            Assert.assertNotEquals(segment.getFreeListHead(), initialFreeListHead);
        }
        Assert.assertEquals(segment.removeCount(), initialRemoveCount);
        Assert.assertEquals(segment.hitCount(), initialHitCount + 2);
        Assert.assertEquals(segment.missCount(), initialMissCount + 2);

        // put again over existing value while putIfAbsent is true should not overwrite
        Assert.assertFalse(segment.putEntry(key, entry, true, null));
        Assert.assertEquals(segment.putReplaceCount(), initialPutReplaceCount);
        Assert.assertEquals(segment.getEntry(key), entry);
        Assert.assertEquals(segment.size(), initialSize + 1);

        // put again over existing value with a non-matching oldValue will not overwrite
        ByteArrayEntry entry2 = serializer.randomEntry(key.size());
        Assert.assertFalse(segment.putEntry(key, entry2, false, entry2));
        Assert.assertEquals(segment.putReplaceCount(), initialPutReplaceCount);
        Assert.assertEquals(segment.getEntry(key), entry);
        Assert.assertEquals(segment.size(), initialSize + 1);

        // put again with different value and matching oldValue should overwrite
        Assert.assertTrue(segment.putEntry(key, entry2, false, entry));
        Assert.assertEquals(segment.putReplaceCount(), initialPutReplaceCount + 1);
        Assert.assertEquals(segment.getEntry(key), entry2);
        Assert.assertEquals(segment.size(), initialSize + 1);
        Assert.assertEquals(segment.freeListSize(), Math.max(initialFreeListSize - expectedSlotsPerEntry, 0));
        if(initialFreeListHead.isEmpty()) {
            Assert.assertEquals(segment.getFreeListHead(), initialFreeListHead);
        } else {
            Assert.assertNotEquals(segment.getFreeListHead(), initialFreeListHead);
        }
        Assert.assertEquals(segment.hitCount(), initialHitCount + 5);
        Assert.assertEquals(segment.missCount(), initialMissCount + 2);

        // remove
        Assert.assertTrue(segment.removeEntry(key));
        Assert.assertFalse(segment.containsEntry(key));
        Assert.assertNull(segment.getEntry(key));
        Assert.assertFalse(segment.removeEntry(key));
        Assert.assertEquals(segment.size(), initialSize);
        Assert.assertEquals(segment.putAddCount(), initialPutAddCount + 1);
        Assert.assertEquals(segment.putReplaceCount(), initialPutReplaceCount + 1);
        Assert.assertEquals(segment.freeListSize(), Math.max(initialFreeListSize, expectedSlotsPerEntry));
        if(initialFreeListHead.isEmpty()) {
            Assert.assertNotEquals(segment.getFreeListHead(), initialFreeListHead);
        } else {
            Assert.assertEquals(segment.getFreeListHead(), initialFreeListHead);
        }
        Assert.assertEquals(segment.removeCount(), initialRemoveCount + 1);
        Assert.assertEquals(segment.hitCount(), initialHitCount + 5);
        Assert.assertEquals(segment.missCount(), initialMissCount + 4);

    }

    @Test
    public void testChunkAllocations() {
        segment = new SegmentWithMemoryPool<>(builder());

        addEntriesToSegment(noOfEntries);
        int noLargeEntries = noOfEntries / 4;
        addLargeEntriesToSegment(noLargeEntries); // will add an equal number of slots as above, but 1/4 the entries

        // each chunk can hold noOfEntries/2 slots hence four chunks must be allocated.
        Assert.assertEquals(segment.numberOfChunks(), 4);
        Assert.assertEquals(segment.numberOfSlots(), noOfEntries * 2);
        Assert.assertEquals(segment.size(), noOfEntries + noLargeEntries);
        Assert.assertEquals(segment.putAddCount(), noOfEntries + noLargeEntries);
        Assert.assertEquals(segment.freeListSize(), 0);

        // All slots in chunk should be written to and full.
        for (int i = 0; i < segment.numberOfChunks(); i++) {
            Assert.assertEquals(segment.getChunkWriteOffset(i), noOfEntries/noOfChunks * fixedSlotSize);
        }
    }

    @Test
    public void testFreeList() {
        segment = new SegmentWithMemoryPool<>(builder());

        //Add noOfEntries to the segment. This should require chunks.
        List<Record> records = addEntriesToSegment(noOfEntries);
        // 1/4 the entries, but the same number of slots used
        int noLargeEntries = noOfEntries / 4;
        int totalEntries = noOfEntries + noLargeEntries;
        int totalSlots = noOfEntries + (4 * noLargeEntries);
        List<Record> bigRecords = addLargeEntriesToSegment(noLargeEntries);

        // each chunk can hold noOfEntries/2 hence four chunks must be allocated.
        Assert.assertEquals(segment.numberOfChunks(), noOfChunks * 2);
        Assert.assertEquals(segment.size(), totalEntries);
        Assert.assertEquals(segment.putAddCount(), totalEntries);
        Assert.assertEquals(segment.freeListSize(), 0);
        Assert.assertTrue(segment.getFreeListHead().isEmpty());

        // remove all entries from the segment
        // all slots should now be part of the free list.
        Lists.reverse(records).forEach(k -> segment.removeEntry(k.keyBuffer));
        Lists.reverse(bigRecords).forEach(k -> segment.removeEntry(k.keyBuffer));

        Assert.assertEquals(segment.freeListSize(), totalSlots);
        Assert.assertFalse(segment.getFreeListHead().isEmpty());
        Assert.assertEquals(segment.removeCount(), totalEntries);
        Assert.assertEquals(segment.size(), 0);

        // Add noOfEntries to the segment.
        // All entries must be added to slots part of the freelist.
        records = addEntriesToSegment(noOfEntries);
        bigRecords = addLargeEntriesToSegment(noLargeEntries);

        Assert.assertEquals(segment.numberOfChunks(), noOfChunks * 2);
        Assert.assertEquals(segment.size(), totalEntries);
        Assert.assertEquals(segment.freeListSize(), 0);

        // after all slots in the free list are used head should point to
        // an empty list.
        Assert.assertTrue(segment.getFreeListHead().isEmpty());

        // remove only some of the elements.
        Random r = new Random();
        int elementsRemoved = 0;
        int bigElementsRemoved = 0;
        for (int i = 0; i < noOfEntries/3; i++) {
            if(segment.removeEntry(records.get(r.nextInt(records.size())).keyBuffer))
                elementsRemoved++;
        }
        for (int i = 0; i < noLargeEntries/3; i++) {
            if(segment.removeEntry(bigRecords.get(r.nextInt(bigRecords.size())).keyBuffer))
                bigElementsRemoved++;
        }

        Assert.assertEquals(segment.freeListSize(), elementsRemoved + (bigElementsRemoved * 4));
        Assert.assertFalse(segment.getFreeListHead().isEmpty());
        Assert.assertEquals(segment.size(), totalEntries - (elementsRemoved + bigElementsRemoved));

        // add removed elements back.
        addEntriesToSegment(elementsRemoved);
        addLargeEntriesToSegment(bigElementsRemoved);

        Assert.assertEquals(segment.numberOfChunks(), noOfChunks * 2);
        Assert.assertEquals(segment.size(), totalEntries);
        Assert.assertEquals(segment.freeListSize(), 0);
        Assert.assertTrue(segment.getFreeListHead().isEmpty());
    }

    @Test(expectedExceptions = OutOfMemoryError.class, expectedExceptionsMessageRegExp = "Each segment can have at most 255 chunks.")
    public void testOutOfMemoryException() {
        // Each segment can have only 255 chunks.
        // we add more that that.
        noOfEntries = 255 + 1;

        segment = new SegmentWithMemoryPool<>(builder().memoryPoolChunkSize(fixedSlotSize));
        addEntriesToSegment(noOfEntries);
    }

    @Test
    public void testReadFromAllChunks() {
        // Each segment can have only 255 chunks.
        noOfEntries = 255;

        segment = new SegmentWithMemoryPool<>(builder().memoryPoolChunkSize(fixedSlotSize));
        List<Record> added = addEntriesToSegment(noOfEntries);
        Assert.assertEquals(255, added.size());
        for (Record record: added) {
            Assert.assertTrue(segment.containsEntry(record.keyBuffer));
            Assert.assertEquals(segment.getEntry(record.keyBuffer), record.entry);
        }
    }

    @Test
    public void testClear() {
     // Each segment can have only 255 chunks.
        noOfEntries = 255;

        segment = new SegmentWithMemoryPool<>(builder().memoryPoolChunkSize(fixedSlotSize));
        List<Record> added = addEntriesToSegment(noOfEntries);
        Assert.assertEquals(255, added.size());
        for (Record record: added) {
            Assert.assertTrue(segment.containsEntry(record.keyBuffer));
            Assert.assertEquals(segment.getEntry(record.keyBuffer), record.entry);
        }
        segment.clear();
        for (Record record: added) {
            Assert.assertFalse(segment.containsEntry(record.keyBuffer));
            Assert.assertEquals(segment.getEntry(record.keyBuffer), null);
        }

    }

    @Test
    public void testReplace() {
        noOfEntries = 1000;
        noOfChunks = 10;

        segment = new SegmentWithMemoryPool<>(builder());

        List<Record> records = addEntriesToSegment(800); // takes 800 slots
        List<Record> largeRecords = addLargeEntriesToSegment(50); // takes 50 * 4 = 200 slots
        int totalEntries = 850;

        Assert.assertEquals(segment.numberOfChunks(), noOfChunks);
        Assert.assertEquals(segment.size(), totalEntries);
        Assert.assertEquals(segment.putAddCount(), totalEntries);
        Assert.assertEquals(segment.freeListSize(), 0);

        Stream.concat(records.stream(), largeRecords.stream()).forEach(record -> {
            ByteArrayEntry newEntry = serializer.randomEntry(record.keyBuffer.size());
            Assert.assertTrue(segment.putEntry(record.keyBuffer, newEntry, false, record.entry));
        });

        // we have replaced all values. no new chunks should
        // have been allocated.
        Assert.assertEquals(segment.numberOfChunks(), noOfChunks);
        Assert.assertEquals(segment.size(), totalEntries);
        Assert.assertEquals(segment.putAddCount(), totalEntries);
        Assert.assertEquals(segment.freeListSize(), 0);
        Assert.assertEquals(segment.putReplaceCount(), totalEntries);

        // All slots in chunk should be written to.
        for (int i = 0; i < segment.numberOfChunks(); i++) {
            Assert.assertEquals(segment.getChunkWriteOffset(i), noOfEntries/noOfChunks * fixedSlotSize);
        }
    }

    @Test
    public void testLongChains() {
        noOfEntries = 2000;
        noOfChunks = 4;

        segment = new SegmentWithMemoryPool<>(OffHeapHashTableBuilder
                .newBuilder(serializer)
                .fixedKeySize(fixedKeySize)
                .memoryPoolChunkSize(noOfEntries/noOfChunks * fixedSlotSize)
                .hashTableSize(256) // small table, for long chains
                .loadFactor(10)  // so large it won't rehash
            );

        Record testy = createRecord(4);
        List<Record> small1 = addEntriesToSegment(500);
        List<Record> large1 = addMaliciousLargeEntriesToSegment(500, testy.keyBuffer.buffer);
        List<Record> small2 = addEntriesToSegment(500);
        List<Record> large2 = addLargeEntriesToSegment(500);

        // if this is not null, it means the segment 'search' algorithm did not properly skip over
        // the extended key slot when matching keys, as the MaliciousLargeEntries have keys that mimic
        // the 'testy' record
        Assert.assertNull(segment.getEntry(testy.keyBuffer), "found fake key in extended key slot, should not have");
        Assert.assertEquals(segment.rehashes(), 0);

        // we should have long chains with small single slot keys mixed with the large multi-slot keys

        containsAll(small1);
        containsAll(large1);
        containsAll(small2);
        containsAll(large2);
    }

    @Test
    public void testRehashSmallKeys() {
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
        List<Record> records = addEntriesToSegment(noOfEntries);

        Assert.assertEquals(segment.size(), noOfEntries);
        Assert.assertEquals(segment.rehashes(), 3);
        Assert.assertEquals(segment.putAddCount(), noOfEntries);

        containsAll(records);
    }

    @Test
    public void testRehashMixedKeys() {
        noOfEntries = 100_000;
        noOfChunks = 10;

        int numSmallKeys = 60_000;
        int numLargeKeys = 10_000; // long keys take 4 slots, so 100_000 total slots used
        int totalKeys = numSmallKeys + numLargeKeys; // 70_000

        OffHeapHashTableBuilder<ByteArrayEntry> builder = OffHeapHashTableBuilder
            .newBuilder(serializer)
            .fixedKeySize(fixedKeySize)
            // chunkSize set such that noOfEntries/2 slots fill one chunk.
            .memoryPoolChunkSize(noOfEntries/noOfChunks * fixedSlotSize)
            .hashTableSize(totalKeys/10) // size of table less than number of entries, this will trigger multiple rehashes.
            .loadFactor(1.25f); // load factor ensures many chains of keys per hash table slot

        segment = new SegmentWithMemoryPool<>(builder);
        // rehash will only trigger with large keys present if added first, so all rehash cycles will contain large keys
        // and most will contain a mix
        List<Record> largeRecords = addLargeEntriesToSegment(numLargeKeys);
        List<Record> records = addEntriesToSegment(numSmallKeys);

        Assert.assertEquals(segment.size(), totalKeys);
        Assert.assertEquals(segment.numberOfSlots(), noOfEntries);
        Assert.assertEquals(segment.rehashes(), 3); // (8192 * 1.1) ~9000 -> ~18000 -> ~36000  -> ~72000
        Assert.assertEquals(segment.putAddCount(), totalKeys);

        containsAll(records);
        containsAll(largeRecords);
    }

    private void containsAll(List<Record> records) {
        records.forEach(r -> Assert.assertEquals(segment.getEntry(r.keyBuffer), r.entry));
    }


    private List<Record> addEntriesToSegment(int noOfEntries) {
        List<Record> records = new ArrayList<>();
        for (int i = 0; i < noOfEntries; i++) {
            Record rec = createRecord(fixedKeySize - (i & 0x1)); // 50% are one byte smaller, so not all slots are full
            records.add(rec);
            segment.putEntry(rec.keyBuffer, rec.entry, true, null);
        }
        return records;
    }

    // generate entries that require four slots per key
    private List<Record> addLargeEntriesToSegment(int noOfEntries) {
        List<Record> records = new ArrayList<>();
        for (int i = 0; i < noOfEntries; i++) {
            Record rec = createRecord((4 *fixedKeySize) + (3 * serializer.entrySize()) - (i & 1));
            records.add(rec);
            segment.putEntry(rec.keyBuffer, rec.entry, true, null);
        }
        return records;
    }

    // generate entries for large keys where the extended blocks appear to be valid ordinary blocks with the given fake key.
    private List<Record> addMaliciousLargeEntriesToSegment(int noOfEntries, byte[] fakeKey) {
        List<Record> records = new ArrayList<>();
        for (int i = 0; i < noOfEntries; i++) {
            Record rec = createMaliciousRecord(fakeKey, (4 *fixedKeySize) + (3 * serializer.entrySize()) - (i & 1));
            records.add(rec);
            segment.putEntry(rec.keyBuffer, rec.entry, true, null);
        }
        return records;
    }

    private Record createRecord(byte[] key) {
        KeyBuffer k = new KeyBuffer(key);
        k.finish(hasher);
        ByteArrayEntry entry = serializer.randomEntry(key.length);
        return new Record(k, entry);
    }

    private Record createRecord(int keySize) {
        byte[] key = HashTableTestUtils.randomBytes(keySize);
        return createRecord(key);
    }

    private Record createMaliciousRecord(byte[] fakeKey, int keySize) {
        byte[] key = HashTableTestUtils.randomBytes(keySize);
        long address = Uns.allocate(5);
        try {
            HashEntry.serializeSizes(address, (short)fakeKey.length, 16);
            Uns.copyMemory(address, 0, key, fixedKeySize, 5);
        } finally {
            Uns.free(address);
        }
        System.arraycopy(fakeKey, 0, key, fixedKeySize + 5, fakeKey.length);
        return createRecord(key);
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
