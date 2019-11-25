/*
 * Copyright 2018, Oath Inc
 * Licensed under the terms of the Apache License 2.0. Please refer to accompanying LICENSE file for terms.
 */

// This code is a derivative work heavily modified from the OHC project. See NOTICE file for copyright and license.

package com.oath.halodb;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import com.google.common.primitives.Longs;

final class HashTableTestUtils {
    static int manyCount = 20000;

    static List<KeyEntryPair> fillMany(OffHeapHashTable<ByteArrayEntry> cache, ByteArrayEntrySerializer serializer)
    {
        return fill(cache, serializer, manyCount);
    }

    static List<KeyEntryPair> fill(OffHeapHashTable<ByteArrayEntry> cache, ByteArrayEntrySerializer serializer, int count)
    {
        List<KeyEntryPair> many = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            byte[] key = Longs.toByteArray(i);
            ByteArrayEntry entry = serializer.randomEntry(key.length);
            cache.put(key, entry);
            many.add(new KeyEntryPair(key, entry));
        }

        return many;
    }

    static byte[] randomBytes(int len)
    {
        Random r = new Random();
        byte[] arr = new byte[len];
        r.nextBytes(arr);
        return arr;
    }

    static class KeyEntryPair {
        byte[] key;
        ByteArrayEntry entry;

        KeyEntryPair(byte[] key, ByteArrayEntry entry) {
            this.key = key;
            this.entry = entry;
        }
    }
}
