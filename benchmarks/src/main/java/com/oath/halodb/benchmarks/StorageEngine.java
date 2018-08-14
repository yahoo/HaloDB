package com.oath.halodb.benchmarks;

public interface StorageEngine {

    void put(byte[] key, byte[] value);

    default String stats() { return "";}

    byte[] get(byte[] key);

    default void delete(byte[] key) {};

    void open();

    void close();

    default long size() {return 0;}

    default void printStats() {

    }
}
