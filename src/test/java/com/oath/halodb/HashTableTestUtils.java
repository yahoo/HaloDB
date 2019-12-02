/*
 * Copyright 2018, Oath Inc
 * Licensed under the terms of the Apache License 2.0. Please refer to accompanying LICENSE file for terms.
 */

// This code is a derivative work heavily modified from the OHC project. See NOTICE file for copyright and license.

package com.oath.halodb;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

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
            byte[] key = HashTableTestUtils.randomBytesOfRange(6, 8, 40);
            ByteArrayEntry entry = serializer.randomEntry(key.length);
            cache.put(key, entry);
            many.add(new KeyEntryPair(key, entry));
        }

        return many;
    }

    /** return a byte[] that has a 50% chance of being in [min, pivot] and a 50% chance of being in (pivot, max] **/
    static byte[] randomBytesOfRange(int min, int pivot, int max) {
        Random r = new Random();
        int size;
        if (r.nextBoolean()) {
            size = min + r.nextInt(pivot - min + 1);
        } else {
            size = pivot + 1 + r.nextInt(max - pivot);
        }
        return randomBytes(size);
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
