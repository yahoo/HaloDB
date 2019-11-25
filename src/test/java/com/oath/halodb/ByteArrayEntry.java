package com.oath.halodb;

import java.util.Arrays;

class ByteArrayEntry implements HashEntry {
    final short keySize;
    final byte[] bytes;

    public ByteArrayEntry(int keySize, byte[] bytes) {
        this.keySize = Utils.validateKeySize(keySize);
        this.bytes = bytes;
    }

    @Override
    public short getKeySize() {
        return keySize;
    }

    @Override
    public int hashCode() {
        return (31 * keySize) + Arrays.hashCode(bytes);
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
        return keySize == other.keySize && Arrays.equals(bytes, other.bytes);
    }
}