/*
 * Copyright 2018, Oath Inc
 * Licensed under the terms of the Apache License 2.0. Please refer to accompanying LICENSE file for terms.
 */

package com.oath.halodb;

import java.util.Arrays;
import java.util.Objects;

public class Record {
    private final byte[] key, value;

    public Record(byte[] key, byte[] value) {
        Utils.validateKeySize(key.length);
        Utils.validateValueSize(value.length);
        this.key = key;
        this.value = value;
    }

    public byte[] getKey() {
        return key;
    }

    public byte[] getValue() {
        return value;
    }

    @Override
    public final boolean equals(Object obj) {
        // final, all child classes only check key/value contents and are mutually compatible here
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof Record)) {
            return false;
        }
        Record record = (Record)obj;
        return Arrays.equals(getKey(), record.getKey()) && Arrays.equals(getValue(), record.getValue());
    }

    @Override
    public final int hashCode() {
        return Objects.hash(Arrays.hashCode(key), Arrays.hashCode(value));
    }
}
