package com.oath.halodb;

import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.locks.AbstractQueuedSynchronizer;

/**
 * CompactionThreadLock is a reentrant lock used by CompactionManager for synchronizing
 * start/stop/pause/resume/restart operations which require mutual exclusion.
 *
 * This class was written as it was difficult to achieve mutual exclusion
 * for those operations and to reason about their concurrent execution with the concurrency
 * primitives that are part of Java's standard library.
 *
 * Proper synchronization was complicated primarily by the fact that when the compaction
 * thread crashes we have to restart it and this can happen anytime, even when any of the above
 * methods are called.
 *
 * A CompactionThreadLock instance provides a single reentrant lock which can be acquired/released
 * from start, stop and restart operations. Although locks are acquired/released using different
 * methods, the underlying lock is the same for all methods.
 *
 * A lock acquired using acquireXXXXLock() can be released only by releaseXXXXLock().
 *
 * When CompactionManager tries to restart a crashed compaction thread we need to check
 * if a stop() operation is already in progress and has acquired this lock. If true, then
 * the acquireRestartLock() operation would not wait and return immediately.
 *
 * In all other scenarios, if the lock is already held by another thread, acquire operation would would cause the
 * calling thread to wait until the lock has been released.
 *
 * The lock is also reentrant for all the acquire operations. For e.g. if a thread
 * acquired the lock with acquireRestartLock it can then also call any of the other
 * acquire operations while holding the lock. 
 *
 */
class CompactionThreadLock {

    void acquireStartLock() {
        sync.lock(lockedByStart);
    }

    void releaseStartLock() {
        sync.release(lockedByStart);
    }

    void acquireStopLock() {
        sync.lock(lockedByStop);
    }

    void releaseStopLock() {
        sync.release(lockedByStop);
    }

    /**
     * If the lock was acquired by another thread using acquireStopLock
     * then this method will return false immediately. Otherwise the thread
     * will wait till the lock is released and return true when it acquires the lock.
     */
    boolean acquireRestartLock() {
        return sync.lockForRestart();
    }

    void releaseRestartLock() {
        sync.release(lockedByRestart);
    }

    Thread getOwner() {
        return sync.getOwner();
    }

    int getAcquireCount() {
        return sync.getAcquireCount();
    }

    boolean hasQueuedThread(Thread thread) {
        return sync.isQueued(thread);
    }


    // internal methods and constants.

    private static final int unlocked = 0;
    private static final int lockedByStart = 1;
    private static final int lockedByStop = 2;
    private static final int lockedByRestart = 3;

    private final Sync sync = new Sync();

    private static class Sync extends AbstractQueuedSynchronizer {
        private final AtomicIntegerArray lockAcquireCount = new AtomicIntegerArray(4);

        void lock(int state) {
            if (compareAndSetState(unlocked, state)) {
                lockAcquireCount.incrementAndGet(state);
                setExclusiveOwnerThread(Thread.currentThread());
            }
            else
                acquire(state);
        }

        boolean lockForRestart() {
            if (compareAndSetState(unlocked, lockedByRestart)) {
                lockAcquireCount.incrementAndGet(lockedByRestart);
                setExclusiveOwnerThread(Thread.currentThread());
                return true;
            }
            else if (getState() != lockedByStop) {
                acquire(lockedByRestart);
                return true;
            }
            else {
                return false;
            }
        }

        @Override
        protected boolean tryAcquire(int state) {
            final Thread thread = Thread.currentThread();
            int currentState = getState();
            if (currentState == unlocked) {
                if (compareAndSetState(unlocked, state)) {
                    setExclusiveOwnerThread(thread);
                    lockAcquireCount.incrementAndGet(state);
                    return true;
                }
            }
            else if (thread == getExclusiveOwnerThread()) {
                if (lockAcquireCount.incrementAndGet(state) < 0)
                    throw new Error("Maximum lock count exceeded");
                return true;
            }
            return false;
        }

        @Override
        protected final boolean tryRelease(int state) {
            if (Thread.currentThread() != getExclusiveOwnerThread()) {
                throw new IllegalMonitorStateException("Lock is held by another thread");
            }
            if (lockAcquireCount.get(state) == 0) {
                throw new IllegalMonitorStateException(
                    "Already released"
                );
            }
            lockAcquireCount.decrementAndGet(state);

            for (int i=0; i<lockAcquireCount.length(); i++) {
                if (lockAcquireCount.get(i) != 0)
                    return false;
            }

            setExclusiveOwnerThread(null);
            setState(unlocked);
            return true;
        }

        Thread getOwner() {
            return getState() == 0 ? null : getExclusiveOwnerThread();
        }

        int getAcquireCount() {
            int count = 0;
            for (int i=0; i<4; i++) {
                count += lockAcquireCount.get(i);
            }
            return count;
        }
    }
}
