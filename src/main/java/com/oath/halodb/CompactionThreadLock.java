package com.oath.halodb;

import java.util.concurrent.atomic.AtomicInteger;
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

    /**
     * Queries whether the given thread is waiting to acquire this
     * lock. Note that because cancellations may occur at any time, a
     * {@code true} return does not guarantee that this thread
     * will ever acquire this lock.  This method is designed primarily for use
     * in monitoring of the system state.
     *
     * @param thread the thread
     * @return {@code true} if the given thread is queued waiting for this lock
     * @throws NullPointerException if the thread is null
     */
     boolean hasQueuedThread(Thread thread) {
        return sync.isQueued(thread);
    }

    private static class Sync extends AbstractQueuedSynchronizer {

        private final AtomicInteger acquireCount = new AtomicInteger(0);

        void lock(int state) {
            if (compareAndSetState(unlocked, state)) {
                acquireCount.incrementAndGet();
                setExclusiveOwnerThread(Thread.currentThread());
            }
            else
                acquire(state);
        }

        boolean lockForRestart() {
            if (compareAndSetState(unlocked, lockedByRestart)) {
                acquireCount.incrementAndGet();
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
                    acquireCount.incrementAndGet();
                    setExclusiveOwnerThread(thread);
                    return true;
                }
            }
            else if (thread == getExclusiveOwnerThread()) {
                if (getState() != state) {
                    throw new IllegalMonitorStateException();
                }
                acquireCount.incrementAndGet();
                if (acquireCount.get() < 0) // overflow
                    throw new Error("Maximum lock count exceeded");
                return true;
            }
            return false;
        }

        @Override
        protected final boolean tryRelease(int state) {
            if (Thread.currentThread() != getExclusiveOwnerThread() || getState() != state) {
                throw new IllegalMonitorStateException();
            }
            if(acquireCount.decrementAndGet() == 0) {
                setExclusiveOwnerThread(null);
                setState(unlocked);
                return true;
            }
            return false;
        }

        Thread getOwner() {
            return getState() == 0 ? null : getExclusiveOwnerThread();
        }

        int getAcquireCount() {
            return acquireCount.get();
        }
    }
}
