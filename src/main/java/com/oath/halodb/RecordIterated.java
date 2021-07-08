/*
 * Copyright 2018, Oath Inc
 * Licensed under the terms of the Apache License 2.0. Please refer to accompanying LICENSE file for terms.
 */

package com.oath.halodb;

/** An internal Record variant that represents a record iterated over by HaloDb. **/
class RecordIterated  extends Record {
    private final long sequenceNumber;

    RecordIterated(byte[] key, byte[] value, long sequenceNumber) {
        super(key, value);
        this.sequenceNumber = sequenceNumber;
    }

    long getSequenceNumber() {
        return sequenceNumber;
    }
}
