/*
 * Copyright 2018, Oath Inc
 * Licensed under the terms of the Apache License 2.0. Please refer to accompanying LICENSE file for terms.
 */

// This code is a derivative work heavily modified from the OHC project. See NOTICE file for copyright and license.

package com.oath.halodb;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.nio.ByteBuffer;

import org.testng.annotations.AfterMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.google.common.hash.HashCode;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;

public class KeyBufferTest
{
    @AfterMethod(alwaysRun = true)
    public void deinit()
    {
        Uns.clearUnsDebugForTest();
    }

    @DataProvider
    public Object[][] hashes()
    {
        return new Object[][]{
        { HashAlgorithm.MURMUR3 },
        { HashAlgorithm.CRC32 },
        // TODO { HashAlgorithm.XX }
        };
    }

    @Test(dataProvider = "hashes")
    public void testHashFinish(HashAlgorithm hashAlgorithm) throws Exception
    {

        ByteBuffer buf = ByteBuffer.allocate(12);
        byte[] ref = TestUtils.generateRandomByteArray(10);
        buf.put((byte) (42 & 0xff));
        buf.put(ref);
        buf.put((byte) (0xf0 & 0xff));

        KeyBuffer out = new KeyBuffer(buf.array());
        out.finish(com.oath.halodb.Hasher.create(hashAlgorithm));

        Hasher hasher = hasher(hashAlgorithm);
        hasher.putByte((byte) 42);
        hasher.putBytes(ref);
        hasher.putByte((byte) 0xf0);
        long longHash = hash(hasher);

        assertEquals(out.hash(), longHash);
    }

    private long hash(Hasher hasher)
    {
        HashCode hash = hasher.hash();
        if (hash.bits() == 32)
        {
            long longHash = hash.asInt();
            longHash = longHash << 32 | (longHash & 0xffffffffL);
            return longHash;
        }
        return hash.asLong();
    }

    private Hasher hasher(HashAlgorithm hashAlgorithm)
    {
        switch (hashAlgorithm)
        {
            case MURMUR3:
                return Hashing.murmur3_128().newHasher();
            case CRC32:
                return Hashing.crc32().newHasher();
            default:
                throw new IllegalArgumentException();
        }
    }

    @Test(dataProvider = "hashes", dependsOnMethods = "testHashFinish")
    public void testHashFinish16(HashAlgorithm hashAlgorithm) throws Exception
    {

        byte[] ref = TestUtils.generateRandomByteArray(14);
        ByteBuffer buf = ByteBuffer.allocate(16);
        buf.put((byte) (42 & 0xff));
        buf.put(ref);
        buf.put((byte) (0xf0 & 0xff));
        KeyBuffer out = new KeyBuffer(buf.array());
        out.finish(com.oath.halodb.Hasher.create(hashAlgorithm));

        Hasher hasher = hasher(hashAlgorithm);
        hasher.putByte((byte) 42);
        hasher.putBytes(ref);
        hasher.putByte((byte) 0xf0);
        long longHash = hash(hasher);

        assertEquals(out.hash(), longHash);
    }

    @Test(dataProvider = "hashes", dependsOnMethods = "testHashFinish16")
    public void testHashRandom(HashAlgorithm hashAlgorithm) throws Exception
    {
        for (int i = 1; i < 4100; i++)
        {
            for (int j = 0; j < 10; j++)
            {

                byte[] ref = TestUtils.generateRandomByteArray(i);
                ByteBuffer buf = ByteBuffer.allocate(i);
                buf.put(ref);
                KeyBuffer out = new KeyBuffer(buf.array());
                out.finish(com.oath.halodb.Hasher.create(hashAlgorithm));

                Hasher hasher = hasher(hashAlgorithm);
                hasher.putBytes(ref);
                long longHash = hash(hasher);

                assertEquals(out.hash(), longHash);
            }
        }
    }

    @Test
    public void testSameKey() {

        int keyLength = 8;
        byte[] randomKey = TestUtils.generateRandomByteArray(keyLength);
        compareKey(randomKey);

        keyLength = 9;
        randomKey = TestUtils.generateRandomByteArray(keyLength);
        compareKey(randomKey);

        for (int i = 0; i < 128; i++) {
            randomKey = TestUtils.generateRandomByteArray();
            keyLength = randomKey.length;
            if (keyLength == 0)
                continue;
            compareKey(randomKey);
        }

    }

    private void compareKey(byte[] randomKey) {
        ByteArrayEntrySerializer serializer = ByteArrayEntrySerializer.ofSize(13);
        ByteArrayEntry randomEntry = serializer.randomEntry(randomKey.length);
        long entryOffset = NonMemoryPoolHashEntries.ENTRY_OFF_DATA;
        long locationOffset = entryOffset + serializer.sizesSize();
        long keyOffset = entryOffset + serializer.entrySize();

        long adr = Uns.allocate(keyOffset + randomKey.length, true);
        try {
            KeyBuffer key = new KeyBuffer(randomKey);
            key.finish(com.oath.halodb.Hasher.create(HashAlgorithm.MURMUR3));

            NonMemoryPoolHashEntries.init(adr);
            randomEntry.serializeSizes(adr + entryOffset);
            randomEntry.serializeLocation(adr + locationOffset);
            Uns.setMemory(adr, keyOffset, randomKey.length, (byte) 0);

            assertFalse(SegmentNonMemoryPool.sameKey(randomKey, adr, serializer));

            Uns.copyMemory(randomKey, 0, adr, keyOffset, randomKey.length);
            NonMemoryPoolHashEntries.init(adr);
            assertTrue(SegmentNonMemoryPool.sameKey(randomKey, adr , serializer));
        } finally {
            Uns.free(adr);
        }
    }
}
