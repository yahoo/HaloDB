package com.oath.halodb;

import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.locks.AbstractQueuedSynchronizer;

class CompactionThreadLock {

    private static final int unlocked = 0;
    private static final int lockedByStart = 1;
    private static final int lockedByStop = 2;
    private static final int lockedByRestart = 3;

    private final Sync sync = new Sync();

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
