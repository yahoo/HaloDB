package com.oath.halodb;

import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.concurrent.CountDownLatch;

public class CompactionThreadLockTest {

    @Test
    public void testLockBehavior() throws InterruptedException {
        final CompactionThreadLock lock = new CompactionThreadLock();
        final Thread main = Thread.currentThread();

        lock.acquireStopLock();

        Thread t = new Thread(() -> {
            Assert.assertEquals(lock.getOwner(), main);
            lock.acquireStopLock();
            Assert.assertEquals(lock.getOwner(), Thread.currentThread());
            lock.releaseStopLock();
            Assert.assertNull(lock.getOwner());
        });
        t.start();
        Assert.assertEquals(lock.getOwner(), main);
        waitForQueuedThread(lock, t, 10_000);
        lock.releaseStopLock();
        t.join();
    }

    @Test
    public void testReentrantBehavior() {
        final CompactionThreadLock lock = new CompactionThreadLock();

        testReentrantBehavior(lock, lock::acquireStartLock, lock::releaseStartLock);
        testReentrantBehavior(lock, lock::acquireStopLock, lock::releaseStopLock);
        testReentrantBehavior(lock, lock::acquireRestartLock, lock::releaseRestartLock);
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

    private void shouldThrowException(Runnable releaseMethod) {
        try {
            releaseMethod.run();
            Assert.fail("Should throw an exception");
        } catch (Exception e) {}
    }

    /**
     * Spin-waits until lock.hasQueuedThread(t) becomes true.
     */
    void waitForQueuedThread(CompactionThreadLock lock, Thread t, long waitTimeInMs) {
        long startTime = System.currentTimeMillis();
        while (!lock.hasQueuedThread(t)) {
            if (System.currentTimeMillis() - startTime > waitTimeInMs)
                throw new AssertionError("timed out");
            Thread.yield();
        }
    }

    public void testLock() {

        final CompactionThreadLock lock = new CompactionThreadLock();

        Thread t = new Thread(() -> {
            lock.acquireStartLock();
            System.out.println("Got start lock in thread.");
            try {
                Thread.sleep(20000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            lock.releaseStartLock();
            System.out.println("Released lock in thread.");
        });
        t.setName("worker");
        t.start();
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        lock.acquireStartLock();
        System.out.println("Got start lock.");
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        lock.releaseStartLock();
        System.out.println("Released lock.");

        try {
            t.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        Assert.assertTrue(1 == 1);

    }

}
