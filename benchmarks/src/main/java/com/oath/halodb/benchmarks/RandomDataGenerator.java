/*
 * Copyright 2018, Oath Inc
 * Licensed under the terms of the Apache License 2.0. Please refer to accompanying LICENSE file for terms.
 */
package com.oath.halodb.benchmarks;

import java.util.Random;

public class RandomDataGenerator {

    private final byte[] data;
    private static final int size = 1003087;
    private int position = 0;

    public RandomDataGenerator(int seed) {
        this.data = new byte[size];
        Random random = new Random(seed);
        random.nextBytes(data);
    }

    public byte[] getData(int length) {
        byte[] b = new byte[length];

        for (int i = 0; i < length; i++) {
            if (position >= size) {
                position = 0;
            }

            b[i] = data[position++];
        }

        return b;
    }
}
