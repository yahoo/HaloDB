package com.oath.halodb;

class ByteArrayEntrySerializer implements HashEntrySerializer<ByteArrayEntry> {

    private final int arraySize;
    private final boolean failOnSerialize;

    ByteArrayEntrySerializer(int arraySize, boolean failOnSerialize) {
      this.arraySize = arraySize;
      this.failOnSerialize = failOnSerialize;
    }

    @Override
    public short readKeySize(long address) {
        return Uns.getShort(address, 0);
    }

    @Override
    public void serialize(ByteArrayEntry entry, long address) {
        if (failOnSerialize) {
            throw new RuntimeException("boom");
        }
        validateArraySize(entry.bytes);
        Uns.putShort(address, 0, entry.keySize);
        Uns.copyMemory(entry.bytes, 0, address, 2, arraySize);
    }

    @Override
    public ByteArrayEntry deserialize(long address) {
        short keySize = Uns.getShort(address, 0);
        byte[] bytes = new byte[arraySize];
        Uns.copyMemory(address, 2, bytes, 0, arraySize);
        return new ByteArrayEntry(keySize, bytes);
    }

    @Override
    public boolean compare(ByteArrayEntry entry, long address) {
        return deserialize(address).equals(entry);
    }

    @Override
    public int fixedSize() {
        return arraySize + 2;
    }

    static ByteArrayEntrySerializer ofSize(int size) {
        return new ByteArrayEntrySerializer(size, false);
    }

    static ByteArrayEntrySerializer ofSizeFailSerialize(int size) {
        return new ByteArrayEntrySerializer(size, true);
    }

    public ByteArrayEntry randomEntry(int keySize) {
        return new ByteArrayEntry(keySize, HashTableTestUtils.randomBytes(arraySize));

    }

    public ByteArrayEntry createEntry(int keySize, byte[] bytes) {
        validateArraySize(bytes);
        return new ByteArrayEntry(keySize, bytes);
    }

    private void validateArraySize(byte[] bytes) {
        if (bytes.length != arraySize) {
            throw new IllegalArgumentException("invalid entry size, expected" + arraySize + " but was " + bytes.length);
        }
    }

}
