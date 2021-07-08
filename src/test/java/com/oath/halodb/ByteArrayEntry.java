package com.oath.halodb;

import java.util.Arrays;

class ByteArrayEntry extends HashEntry {
    private final boolean failOnSerialize;
    final byte[] bytes;

    ByteArrayEntry(int keySize, byte[] bytes) {
        this(keySize, bytes, false);
    }

    ByteArrayEntry(int keySize, byte[] bytes, boolean failOnSerialize) {
        super(keySize, bytes.length);
        this.failOnSerialize = failOnSerialize;
        this.bytes = bytes;
    }

    @Override
    public int hashCode() {
        return (31 * getKeySize()) + Arrays.hashCode(bytes);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        ByteArrayEntry other = (ByteArrayEntry) obj;
        return getKeySize() == other.getKeySize() && Arrays.equals(bytes, other.bytes);
    }

    @Override
    void serializeLocation(long locationAddress) {
        if (failOnSerialize) {
            throw new RuntimeException("boom");
        }
        Uns.copyMemory(bytes, 0, locationAddress, 0, bytes.length);
    }

    @Override
    boolean compareLocation(long locationAddress) {
        byte[] data = new byte[getValueSize()];
        Uns.copyMemory(locationAddress, 0, data, 0, data.length);
        return Arrays.equals(bytes,  data);
    }
}