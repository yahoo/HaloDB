/*
 * Copyright 2018, Oath Inc
 * Licensed under the terms of the Apache License 2.0. Please refer to accompanying LICENSE file for terms.
 */

package com.oath.halodb;

import com.google.common.primitives.Longs;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Pulkit Goel
 */
public class SequenceNumberTest extends TestBase {

    @Test(dataProvider = "Options")
    public void testSequenceNumber(HaloDBOptions options) throws Exception {
        String directory = TestUtils.getTestDirectory("SequenceNumberTest", "testSequenceNumber");

        int recordSize = 1024;
        int recordNumber = 100;

        HaloDB db = getTestDB(directory, options);

        byte[] data = new byte[recordSize - Record.Header.HEADER_SIZE - 8 - 8];
        for (int i = 0; i < data.length; i++) {
            data[i] = (byte)i;
        }

        Record[] records = new Record[recordNumber];
        for (int i = 0; i < recordNumber; i++) {
            byte[] key = Longs.toByteArray(i);
            byte[] value = TestUtils.concatenateArrays(data, key);
            records[i] = new Record(key, value);
            db.put(records[i].getKey(), records[i].getValue());
        }

        // Iterate through all records, atleast one record should have 100 as the sequenceNumber
        HaloDBIterator haloDBIterator = db.newIterator();
        List<Long> sequenceNumbers = new ArrayList<>();
        while (haloDBIterator.hasNext()) {
            Record record = haloDBIterator.next();
            sequenceNumbers.add(record.getRecordMetaData().getSequenceNumber());
        }

        MatcherAssert.assertThat(100L, Matchers.isIn(sequenceNumbers));
    }
}
