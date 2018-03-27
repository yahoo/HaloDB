/*
 *      Copyright (C) 2014 Robert Stupp, Koeln, Germany, robert-stupp.de
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
package com.oath.halodb.cache.linked;

import com.google.common.hash.HashCode;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;

import com.oath.halodb.TestUtils;
import com.oath.halodb.cache.HashAlgorithm;

import org.testng.annotations.AfterMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.nio.ByteBuffer;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

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
        KeyBuffer out = new KeyBuffer(12);

        ByteBuffer buf = out.byteBuffer();
        byte[] ref = TestUtils.generateRandomByteArray(10);
        buf.put((byte) (42 & 0xff));
        buf.put(ref);
        buf.put((byte) (0xf0 & 0xff));
        out.finish(com.oath.halodb.cache.linked.Hasher.create(hashAlgorithm));

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
        KeyBuffer out = new KeyBuffer(16);

        byte[] ref = TestUtils.generateRandomByteArray(14);
        ByteBuffer buf = out.byteBuffer();
        buf.put((byte) (42 & 0xff));
        buf.put(ref);
        buf.put((byte) (0xf0 & 0xff));
        out.finish(com.oath.halodb.cache.linked.Hasher.create(hashAlgorithm));

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
                KeyBuffer out = new KeyBuffer(i);

                byte[] ref = TestUtils.generateRandomByteArray(i);
                ByteBuffer buf = out.byteBuffer();
                buf.put(ref);
                out.finish(com.oath.halodb.cache.linked.Hasher.create(hashAlgorithm));

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
        KeyBuffer key = new KeyBuffer(keyLength);
        byte[] randomKey = TestUtils.generateRandomByteArray(keyLength);
        compareKey(key, randomKey);

        keyLength = 9;
        key = new KeyBuffer(keyLength);
        randomKey = TestUtils.generateRandomByteArray(keyLength);
        compareKey(key, randomKey);

        for (int i = 0; i < 128; i++) {
            randomKey = TestUtils.generateRandomByteArray();
            keyLength = randomKey.length;
            if (keyLength == 0)
                continue;
            key = new KeyBuffer(keyLength);
            compareKey(key, randomKey);
        }

    }

    private void compareKey(KeyBuffer key, byte[] randomKey) {

        long adr = Uns.allocate(HashEntries.ENTRY_OFF_DATA + randomKey.length, true);
        try {
            ByteBuffer keyBuffer = key.byteBuffer();
            keyBuffer.put(randomKey);
            key.finish(com.oath.halodb.cache.linked.Hasher.create(HashAlgorithm.MURMUR3));

            HashEntries.init(randomKey.length, adr);
            Uns.setMemory(adr, HashEntries.ENTRY_OFF_DATA, randomKey.length, (byte) 0);

            assertFalse(key.sameKey(adr));

            Uns.copyMemory(randomKey, 0, adr, HashEntries.ENTRY_OFF_DATA, randomKey.length);
            HashEntries.init(randomKey.length, adr);
            assertTrue(key.sameKey(adr));
        } finally {
            Uns.free(adr);
        }
    }
}
