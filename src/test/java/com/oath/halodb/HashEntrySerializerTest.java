/*
 * Copyright 2018, Oath Inc
 * Licensed under the terms of the Apache License 2.0. Please refer to accompanying LICENSE file for terms.
 */

package com.oath.halodb;

import java.util.function.BiPredicate;

import org.testng.Assert;

interface HashEntrySerializerTest {

    default <E extends HashEntry> E testSerDe(E entry, HashEntrySerializer<E> serializer, BiPredicate<E, E> equals) {
        long adr = Uns.allocate(serializer.fixedSize(), true);
        try {
            serializer.serialize(entry, adr);
            Assert.assertTrue(serializer.compare(entry, adr));

            Assert.assertEquals(serializer.readKeySize(adr), entry.getKeySize());

            E fromAdr = serializer.deserialize(adr);
            Assert.assertTrue(serializer.compare(fromAdr, adr));

            Assert.assertEquals(fromAdr.getKeySize(), entry.getKeySize());
            Assert.assertTrue(equals.test(fromAdr, entry));
            return fromAdr;
        } finally {
            Uns.free(adr);
        }
    }
}
