/*
 * Copyright 2018, Oath Inc
 * Licensed under the terms of the Apache License 2.0. Please refer to accompanying LICENSE file for terms.
 */

// This code is a derivative work heavily modified from the OHC project. See NOTICE file for copyright and license.

package com.oath.halodb.cache.linked;

import com.google.common.base.Charsets;
import com.google.common.primitives.Longs;
import com.oath.halodb.cache.CacheSerializer;
import com.oath.halodb.cache.OHCache;
import org.testng.Assert;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

final class TestUtils
{
    public static final long ONE_MB = 1024 * 1024;
    public static final CacheSerializer<String> stringSerializer = new CacheSerializer<String>()
    {
        public void serialize(String s, ByteBuffer buf)
        {
            byte[] bytes = s.getBytes(Charsets.UTF_8);
            buf.put((byte) ((bytes.length >>> 8) & 0xFF));
            buf.put((byte) ((bytes.length >>> 0) & 0xFF));
            buf.put(bytes);
        }

        public String deserialize(ByteBuffer buf)
        {
            int length = (((buf.get() & 0xff) << 8) + ((buf.get() & 0xff) << 0));
            byte[] bytes = new byte[length];
            buf.get(bytes);
            return new String(bytes, Charsets.UTF_8);
        }

        public int serializedSize(String s)
        {
            return writeUTFLen(s);
        }
    };

    public static final CacheSerializer<byte[]> byteArraySerializer = new CacheSerializer<byte[]>()
    {
        @Override
        public void serialize(byte[] value, ByteBuffer buf) {
            buf.put(value);
        }

        @Override
        public byte[] deserialize(ByteBuffer buf) {
            // Cannot use buf.array() as buf is read-only for get() operations.
            byte[] array = new byte[buf.remaining()];
            buf.get(array);
            return array;
        }

        @Override
        public int serializedSize(byte[] value) {
            return value.length;
        }
    };

    public static final CacheSerializer<String> stringSerializerFailSerialize = new CacheSerializer<String>()
    {
        public void serialize(String s, ByteBuffer buf)
        {
            throw new RuntimeException("foo bar");
        }

        public String deserialize(ByteBuffer buf)
        {
            int length = (buf.get() << 8) + (buf.get() << 0);
            byte[] bytes = new byte[length];
            buf.get(bytes);
            return new String(bytes, Charsets.UTF_8);
        }

        public int serializedSize(String s)
        {
            return writeUTFLen(s);
        }
    };
    public static final CacheSerializer<String> stringSerializerFailDeserialize = new CacheSerializer<String>()
    {
        public void serialize(String s, ByteBuffer buf)
        {
            byte[] bytes = s.getBytes(Charsets.UTF_8);
            buf.put((byte) ((bytes.length >>> 8) & 0xFF));
            buf.put((byte) ((bytes.length >>> 0) & 0xFF));
            buf.put(bytes);
        }

        public String deserialize(ByteBuffer buf)
        {
            throw new RuntimeException("foo bar");
        }

        public int serializedSize(String s)
        {
            return writeUTFLen(s);
        }
    };

    public static final CacheSerializer<byte[]> byteArraySerializerFailSerialize = new CacheSerializer<byte[]>()
    {
        public void serialize(byte[] s, ByteBuffer buf)
        {
            throw new RuntimeException("foo bar");
        }

        public byte[] deserialize(ByteBuffer buf)
        {
            byte[] array = new byte[buf.remaining()];
            buf.get(array);
            return array;
        }

        public int serializedSize(byte[] s)
        {
            return s.length;
        }
    };

    static int writeUTFLen(String str)
    {
        int strlen = str.length();
        int utflen = 0;
        int c;

        for (int i = 0; i < strlen; i++)
        {
            c = str.charAt(i);
            if ((c >= 0x0001) && (c <= 0x007F))
                utflen++;
            else if (c > 0x07FF)
                utflen += 3;
            else
                utflen += 2;
        }

        if (utflen > 65535)
            throw new RuntimeException("encoded string too long: " + utflen + " bytes");

        return utflen + 2;
    }

    public static final byte[] dummyByteArray;
    public static final CacheSerializer<Integer> intSerializer = new CacheSerializer<Integer>()
    {
        public void serialize(Integer s, ByteBuffer buf)
        {
            buf.put((byte)(1 & 0xff));
            buf.putChar('A');
            buf.putDouble(42.42424242d);
            buf.putFloat(11.111f);
            buf.putInt(s);
            buf.putLong(Long.MAX_VALUE);
            buf.putShort((short)(0x7654 & 0xFFFF));
            buf.put(dummyByteArray);
        }

        public Integer deserialize(ByteBuffer buf)
        {
            Assert.assertEquals(buf.get(), (byte) 1);
            Assert.assertEquals(buf.getChar(), 'A');
            Assert.assertEquals(buf.getDouble(), 42.42424242d);
            Assert.assertEquals(buf.getFloat(), 11.111f);
            int r = buf.getInt();
            Assert.assertEquals(buf.getLong(), Long.MAX_VALUE);
            Assert.assertEquals(buf.getShort(), 0x7654);
            byte[] b = new byte[dummyByteArray.length];
            buf.get(b);
            Assert.assertEquals(b, dummyByteArray);
            return r;
        }

        public int serializedSize(Integer s)
        {
            return 529;
        }
    };
    public static final CacheSerializer<Integer> intSerializerFailSerialize = new CacheSerializer<Integer>()
    {
        public void serialize(Integer s, ByteBuffer buf)
        {
            throw new RuntimeException("foo bar");
        }

        public Integer deserialize(ByteBuffer buf)
        {
            Assert.assertEquals(buf.get(), (byte) 1);
            Assert.assertEquals(buf.getChar(), 'A');
            Assert.assertEquals(buf.getDouble(), 42.42424242d);
            Assert.assertEquals(buf.getFloat(), 11.111f);
            int r = buf.getInt();
            Assert.assertEquals(buf.getLong(), Long.MAX_VALUE);
            Assert.assertEquals(buf.getShort(), 0x7654);
            byte[] b = new byte[dummyByteArray.length];
            buf.get(b);
            Assert.assertEquals(b, dummyByteArray);
            return r;
        }

        public int serializedSize(Integer s)
        {
            return 529;
        }
    };
    public static final CacheSerializer<Integer> intSerializerFailDeserialize = new CacheSerializer<Integer>()
    {
        public void serialize(Integer s, ByteBuffer buf)
        {
            buf.putInt(s);
        }

        public Integer deserialize(ByteBuffer buf)
        {
            throw new RuntimeException("foo bar");
        }

        public int serializedSize(Integer s)
        {
            return 4;
        }
    };
    static final String big;
    static final String bigRandom;

    static {
        dummyByteArray = new byte[500];
        for (int i = 0; i < TestUtils.dummyByteArray.length; i++)
            TestUtils.dummyByteArray[i] = (byte) ((byte) i % 199);
    }

    static int manyCount = 20000;

    static
    {

        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 1000; i++)
            sb.append("the quick brown fox jumps over the lazy dog");
        big = sb.toString();

        Random r = new Random();
        sb.setLength(0);
        for (int i = 0; i < 30000; i++)
            sb.append((char) (r.nextInt(99) + 31));
        bigRandom = sb.toString();
    }

    static List<KeyValuePair> fillMany(OHCache<byte[]> cache, int fixedValueSize)
    {
        return fill(cache, fixedValueSize, manyCount);
    }

    static List<KeyValuePair> fill(OHCache<byte[]> cache, int fixedValueSize, int count)
    {
        List<KeyValuePair> many = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            byte[] key = Longs.toByteArray(i);
            byte[] value = TestUtils.randomBytes(fixedValueSize);
            cache.put(key, value);
            many.add(new KeyValuePair(key, value));
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

    static class KeyValuePair {
        byte[] key, value;

        KeyValuePair(byte[] key, byte[] value) {
            this.key = key;
            this.value = value;
        }
    }
}
