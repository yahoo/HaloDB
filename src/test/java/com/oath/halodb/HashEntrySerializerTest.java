/*
 * Copyright 2018, Oath Inc
 * Licensed under the terms of the Apache License 2.0. Please refer to accompanying LICENSE file for terms.
 */

package com.oath.halodb;

import java.util.function.BiPredicate;

import org.testng.Assert;

interface HashEntrySerializerTest {

    default <E extends HashEntry> E testSerDe(E entry, HashEntrySerializer<E> serializer, BiPredicate<E, E> equals) {
        long adr = Uns.allocate(serializer.entrySize(), true);
        try {
            entry.serializeSizes(adr);
            Assert.assertTrue(entry.compareSizes(adr));

            long locationAdr = adr + serializer.sizesSize();
            entry.serializeLocation(locationAdr);
            Assert.assertTrue(entry.compareLocation(locationAdr));

            Assert.assertTrue(entry.compare(adr, locationAdr));

            Assert.assertEquals(serializer.readKeySize(adr), entry.getKeySize());

            E fromAdr = serializer.deserialize(adr, locationAdr);

            Assert.assertEquals(fromAdr.getKeySize(), entry.getKeySize());
            Assert.assertEquals(fromAdr.getValueSize(), entry.getValueSize());
            Assert.assertTrue(equals.test(fromAdr, entry));
            return fromAdr;
        } finally {
            Uns.free(adr);
        }
    }
}
