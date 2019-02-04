package com.oath.halodb;

import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.concurrent.atomic.AtomicBoolean;

public class CompactionThreadLockTest {

    @Test
    public void testLockBehavior() throws InterruptedException {
        final CompactionThreadLock lock = new CompactionThreadLock();
        testLockBehavior(lock, lock::acquireStartLock, lock::releaseStartLock);
        testLockBehavior(lock, lock::acquireStopLock, lock::releaseStopLock);
        testLockBehavior(lock, lock::acquireRestartLock, lock::releaseRestartLock);

        testLockBehavior(lock, lock::acquireStartLock, lock::releaseStartLock, lock::acquireStopLock, lock::releaseStopLock);
        testLockBehavior(lock, lock::acquireStartLock, lock::releaseStartLock, lock::acquireRestartLock, lock::releaseRestartLock);

        testLockBehavior(lock, lock::acquireRestartLock, lock::releaseRestartLock, lock::acquireStartLock, lock::releaseStartLock);
        testLockBehavior(lock, lock::acquireRestartLock, lock::releaseRestartLock, lock::acquireStopLock, lock::releaseStopLock);

        testLockBehavior(lock, lock::acquireStopLock, lock::releaseStopLock, lock::acquireStartLock, lock::releaseStartLock);
    }

    @Test
    public void testReentrantBehavior() {
        final CompactionThreadLock lock = new CompactionThreadLock();
        testReentrantBehavior(lock, lock::acquireStartLock, lock::releaseStartLock);
        testReentrantBehavior(lock, lock::acquireStopLock, lock::releaseStopLock);
        testReentrantBehavior(lock, lock::acquireRestartLock, lock::releaseRestartLock);
    }

    @Test
    public void testStopAndRestartLockBehavior() throws InterruptedException {
        final CompactionThreadLock lock = new CompactionThreadLock();
        final Thread main = Thread.currentThread();
        AtomicBoolean threadSuccess = new AtomicBoolean(false);

        Thread t1 = new Thread(() -> {
            Assert.assertEquals(lock.getOwner(), main);
            // return false without waiting since lock is another thread holds a stop lock.
            Assert.assertFalse(lock.acquireRestartLock());
            Assert.assertEquals(lock.getOwner(), main);
            Assert.assertFalse(lock.hasQueuedThread(Thread.currentThread()));
            threadSuccess.set(true);
        });

        // t2 will run when start lock is held, therefore
        // it will wait will start lock is released.
        Thread t2 = new Thread(() -> {
            Assert.assertEquals(lock.getOwner(), main);
            Assert.assertTrue(lock.acquireRestartLock());
            Assert.assertEquals(lock.getOwner(), Thread.currentThread());
            lock.releaseRestartLock();
            Assert.assertNull(lock.getOwner());
            threadSuccess.set(true);
        });

        // acquire and release stop lock.
        // since stop lock is already held t1 will fail to acquire release lock.
        lock.acquireStopLock();
        t1.start();
        Assert.assertEquals(lock.getOwner(), main);
        Assert.assertFalse(lock.hasQueuedThread(t1));
        t1.join(60_000);
        Assert.assertTrue(threadSuccess.get());
        threadSuccess.set(false);
        lock.releaseStopLock();

        // acquire and release start lock.
        lock.acquireStartLock();
        t2.start();
        Assert.assertEquals(lock.getOwner(), main);
        waitForQueuedThread(lock, t2, 60_000);
        Assert.assertTrue(lock.hasQueuedThread(t2));
        lock.releaseStartLock();
        t2.join(60_000);
        Assert.assertTrue(threadSuccess.get());
    }

    @Test
    public void testIllegalMonitorException() {
        final CompactionThreadLock lock = new CompactionThreadLock();

        lock.acquireStartLock();
        shouldThrowException(lock::releaseStopLock);
        shouldThrowException(lock::releaseRestartLock);
        lock.releaseStartLock();


        lock.acquireStopLock();
        shouldThrowException(lock::releaseStartLock);
        shouldThrowException(lock::releaseRestartLock);
        lock.releaseStopLock();


        lock.acquireRestartLock();
        shouldThrowException(lock::releaseStartLock);
        shouldThrowException(lock::releaseStopLock);
        lock.releaseRestartLock();
    }

    private void testReentrantBehavior(CompactionThreadLock lock, Runnable lockMethod, Runnable unlockMethod) {
        Assert.assertNull(lock.getOwner());
        Assert.assertEquals(lock.getAcquireCount(), 0);

        lockMethod.run();
        unlockMethod.run();

        lockMethod.run();
        lockMethod.run();
        Assert.assertEquals(lock.getOwner(), Thread.currentThread());
        Assert.assertEquals(lock.getAcquireCount(), 2);

        unlockMethod.run();
        Assert.assertEquals(lock.getAcquireCount(), 1);
        Assert.assertEquals(lock.getOwner(), Thread.currentThread());

        unlockMethod.run();
        Assert.assertNull(lock.getOwner());
        Assert.assertEquals(lock.getAcquireCount(), 0);
    }

    private void testLockBehavior(CompactionThreadLock lock, Runnable lockMethod, Runnable unlockMethod) throws InterruptedException {
        final Thread main = Thread.currentThread();
        AtomicBoolean threadSuccess = new AtomicBoolean(false);

        Thread t = new Thread(() -> {
            Assert.assertEquals(lock.getOwner(), main);
            lockMethod.run();
            Assert.assertEquals(lock.getOwner(), Thread.currentThread());
            unlockMethod.run();
            Assert.assertNull(lock.getOwner());
            threadSuccess.set(true);
        });

        lockMethod.run();
        t.start();
        Assert.assertEquals(lock.getOwner(), main);
        waitForQueuedThread(lock, t, 60_000);
        Assert.assertTrue(lock.hasQueuedThread(t));
        unlockMethod.run();
        t.join(60_000);
        Assert.assertTrue(threadSuccess.get());
    }

    private void testLockBehavior(CompactionThreadLock lock, Runnable lockMethodA, Runnable unlockMethodA, Runnable lockMethodB, Runnable unlockMethodB) throws InterruptedException {
        final Thread main = Thread.currentThread();
        AtomicBoolean threadSuccess = new AtomicBoolean(false);

        Thread t = new Thread(() -> {
            Assert.assertEquals(lock.getOwner(), main);
            lockMethodB.run();
            Assert.assertEquals(lock.getOwner(), Thread.currentThread());
            unlockMethodB.run();
            Assert.assertNull(lock.getOwner());
            threadSuccess.set(true);
        });

        lockMethodA.run();
        t.start();
        Assert.assertEquals(lock.getOwner(), main);
        waitForQueuedThread(lock, t, 60_000);
        Assert.assertTrue(lock.hasQueuedThread(t));
        unlockMethodA.run();
        t.join(60_000);
        Assert.assertTrue(threadSuccess.get());
    }

    private void shouldThrowException(Runnable releaseMethod) {
        try {
            releaseMethod.run();
            Assert.fail("Should throw an exception");
        } catch (Exception e) {}
    }

    // spins till thread t is queued to lock.
    private void waitForQueuedThread(CompactionThreadLock lock, Thread t, long waitTimeInMs) {
        long startTime = System.currentTimeMillis();
        while (!lock.hasQueuedThread(t)) {
            if (System.currentTimeMillis() - startTime > waitTimeInMs)
                throw new AssertionError("timed out");
            Thread.yield();
        }
    }
}
