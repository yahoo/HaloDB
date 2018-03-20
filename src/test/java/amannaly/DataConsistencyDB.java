package amannaly;

import com.google.common.primitives.Ints;

import org.testng.Assert;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Holds an instance of HaloDB and Java's ConcurrentHashMap.
 * Tests will use this to insert data into both and ensure that
 * the data in HaloDB is correct. 
 *
 * @author Arjun Mannaly
 */

class DataConsistencyDB {

    private final Map<Integer, byte[]> javaMap = new ConcurrentHashMap<>();
    private final HaloDB haloDB;

    private int numberOfLocks = 100;

    private final ReentrantReadWriteLock[] locks;

    DataConsistencyDB(HaloDB haloDB) {
        this.haloDB = haloDB;

        locks = new ReentrantReadWriteLock[numberOfLocks];
        for (int i = 0; i < numberOfLocks; i++) {
            locks[i] = new ReentrantReadWriteLock();
        }
    }

    void put(int key, byte[] value) throws IOException {
        ReentrantReadWriteLock lock = locks[key%numberOfLocks];
        try {
            lock.writeLock().lock();
            javaMap.put(key, value);
            haloDB.put(Ints.toByteArray(key), value);
        }
        finally {
            lock.writeLock().unlock();
        }
    }

    byte[] get(int key) throws IOException {
        ReentrantReadWriteLock lock = locks[key%numberOfLocks];
        try {
            lock.readLock().lock();
            byte[] mapValue = javaMap.get(key);
            byte[] dbValue = haloDB.get(Ints.toByteArray(key));
            Assert.assertEquals(mapValue, dbValue);
            return dbValue;
        }
        finally {
            lock.readLock().unlock();
        }
    }

    void delete(int key) throws IOException {
        ReentrantReadWriteLock lock = locks[key%numberOfLocks];
        try {
            lock.writeLock().lock();
            javaMap.remove(key);
            haloDB.delete(Ints.toByteArray(key));
        }
        finally {
            lock.writeLock().unlock();
        }
    }
}
