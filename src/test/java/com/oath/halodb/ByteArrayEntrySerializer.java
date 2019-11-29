package com.oath.halodb;

class ByteArrayEntrySerializer extends HashEntrySerializer<ByteArrayEntry> {

    private final int arraySize;

    ByteArrayEntrySerializer(int arraySize) {
      this.arraySize = arraySize;
    }

    @Override
    ByteArrayEntry deserialize(long sizeAddress, long locationAddress) {
        int firstWord = Uns.getInt(sizeAddress, 0);
        byte nextByte = Uns.getByte(sizeAddress, 4);
        short keySize = HashEntry.extractKeySize(firstWord);
        int valueSize = HashEntry.extractValueSize(firstWord, nextByte);
        validateArraySize(valueSize);
        byte[] bytes = new byte[valueSize];
        Uns.copyMemory(locationAddress, 0, bytes, 0, arraySize);
        return new ByteArrayEntry(keySize, bytes);
    }

    @Override
    int locationSize() {
        return arraySize;
    }

    @Override
    boolean validSize(ByteArrayEntry entry) {
        return entry.bytes.length == arraySize;
    }

    static ByteArrayEntrySerializer ofSize(int size) {
        return new ByteArrayEntrySerializer(size);
    }

    ByteArrayEntry randomEntry(int keySize) {
        return new ByteArrayEntry(keySize, HashTableTestUtils.randomBytes(arraySize));
    }

    ByteArrayEntry createEntry(int keySize, byte[] bytes) {
        validateArraySize(bytes.length);
        return new ByteArrayEntry(keySize, bytes);
    }

    private void validateArraySize(int length) {
        if (length != arraySize) {
            throw new IllegalArgumentException("invalid entry size, expected" + arraySize + " but was " + length);
        }
    }
}
