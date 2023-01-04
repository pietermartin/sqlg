package org.umlg.sqlg.structure.topology;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.AbstractQueuedSynchronizer;

/**
 * @author <a href="https://github.com/pietermartin">Pieter Martin</a>
 * Date: 2019/01/26
 */
public class CountUpDownLatch {

    private final Sync sync;

    /**
     * Default constructor.
     * <p>
     * Equivalent to {@code new}
     * {@link CountUpDownLatch(int) CountUpDownLatch}{@code (0)}
     */
    public CountUpDownLatch() {
        this(0);
    }

    /**
     * Constructs a new {@code CountUpDownLatch} initialized with the given
     * {@code initialCount}.
     *
     * @param initialCount the initial {@code count}
     * @throws IllegalArgumentException if {@code initialCount} is negative
     */
    public CountUpDownLatch(int initialCount) {

        if (initialCount < 0) {
            throw new IllegalArgumentException("count < 0");
        }

        this.sync = new Sync(initialCount);
    }

    /**
     * Causes the current thread to wait until {@code count} reaches zero,
     * unless the thread is {@linkplain Thread#interrupt interrupted}.
     * <p>
     * If the current {@code count} is already zero, then this method returns
     * immediately.
     * <p>
     * <p>
     * If the current {@code count} is greater than zero, then the current
     * thread becomes disabled for thread scheduling purposes and lies dormant
     * until either:
     * <ul>
     * <li>The count reaches zero due an invocation of {@link #countDown()
     * countDown()}, {@link  #countDown(int) countDown(int}}, or {@link * setCount(int) setCount(int)}.
     * <li>Some other thread {@linkplain Thread#interrupt interrupts} the
     * current thread.
     * </ul>
     * If the current thread:
     * <ul>
     * <li>has its interrupted status set on entry to this method; or
     * <li>is {@linkplain Thread#interrupt interrupted} while waiting,
     * </ul>
     * then {@link InterruptedException} is thrown and the current thread's
     * interrupted status is cleared.
     *
     * @throws InterruptedException if the current thread is interrupted
     *                              while waiting
     */
    public void await() throws InterruptedException {
        sync.acquireSharedInterruptibly(1);
    }

    /**
     * Causes the current thread to wait until {@code count} reaches zero,
     * unless the thread is {@linkplain Thread#interrupt interrupted}, or the
     * specified waiting time elapses.
     * <p>
     * <p>
     * If the current {@code count} is zero, then this method returns
     * immediately with the value {@code true}.
     * <p>
     * <p>
     * If the current {@code count} is greater than zero, then the current
     * thread becomes disabled for thread scheduling purposes and lies dormant
     * until either:
     * <ul>
     * <li>The {@code count} reaches zero due to an invocation of {@link
     * #countDown countDown()}, {@link #countDown(int) countDown(int}}, or {@link * setCount(int) setCount(int)}
     * <li>Some other thread {@linkplain Thread#interrupt interrupts} the
     * current thread
     * <li>The specified waiting time elapses.
     * </ul>
     * If the count reaches zero then the method returns with the value
     * {@code true}.
     * <p>
     * If the current thread:
     * <ul>
     * <li>has its interrupted status set on entry to this method; or
     * <li>is {@linkplain Thread#interrupt interrupted} while waiting,
     * </ul>
     * then {@link InterruptedException} is thrown and the current thread's
     * interrupted status is cleared.
     * <p>
     * If the specified waiting time elapses then the value {@code false} is
     * returned. If the time is less than or equal to zero, the method will not
     * wait at all.
     *
     * @param timeout the maximum time to wait
     * @param unit    the time unit of the {@code timeout} argument
     * @return {@code true} if the count reached zero and {@code false} if the
     * waiting time elapsed before the count reached zero
     * @throws InterruptedException if the current thread is interrupted while
     *                              waiting
     */
    public boolean await(long timeout,
                         TimeUnit unit) throws InterruptedException {
        return sync.tryAcquireSharedNanos(1, unit.toNanos(timeout));
    }

    /**
     * Increments the count of the latch.
     * <p>
     *
     * @return {@code true} if {@code count} transitioned from zero to a new
     * value
     * @throws ArithmeticException when the operation would otherwise cause a
     *                             silent numeric overflow, resulting in a
     *                             negative {@code count}.
     */
    public boolean countUp() {
        return sync.countUp();
    }

    /**
     * Increments the count of the latch by the given {@code amount}.
     * <p>
     *
     * @param amount by which to increment {@code count}
     * @return {@code true} if {@code count} transitioned from zero to a new
     * value
     * @throws ArithmeticException      when the operation would otherwise cause
     *                                  a silent numeric overflow, resulting in
     *                                  a negative {@code count}.
     * @throws IllegalArgumentException if {@code amount is less than one)
     */
    public boolean countUp(int amount) {
        return sync.countUp(amount);
    }

    /**
     * Decrements the count of the latch, releasing all waiting threads if the
     * count transitions to zero.
     * <p>
     * If the current count is zero, no action occurs and false is returned
     * immediately;
     *
     * @return {@code true} if {@code count} transitions to zero
     */
    public boolean countDown() {
        return sync.countDown();
    }

    /**
     * Decrements the {@code count} of the latch by the given {@code amount},
     * releasing all waiting threads if {@code count} transitions to zero.
     * <p>
     * If the current {@code count} is zero, no action occurs and false is
     * returned immediately; otherwise, {@code count} is decremented by the
     * lesser of {@code amount} and current {@code count} (i.e. if
     * {@code amount} is greater than current {@code count}, then new
     * {@code count} is zero, else new {@code count} is current {@code count}
     * minus {@code amount}.
     *
     * @param amount by which to decrement the {@code count}
     * @return {@code true} if {@code count} transitions to zero
     * @throws IllegalArgumentException when {@code amount} is non-positive
     */
    public boolean countDown(int amount) {
        return sync.countDown(amount);
    }

    /**
     * Returns the current count.
     * <p>
     * Because another thread may update {@code count} at any time, typically
     * this should not be used to compute input values for any of the @{code
     * count} mutating methods and instead should be reserved for debugging and
     * testing purposes (e.g. to assert that the current count is the expected
     * count, given a set of know operations has occurred and given that it is
     * known no other threads could be updating the count)
     *
     * @return the current count
     */
    public int getCount() {
        return sync.getCount();
    }

    /**
     * Updates {@code count} to the requested {@code newCount}, returning
     * {@code true} on transition to zero.
     * <p>
     * If {@code newCount} is zero and the current }@code count is zero}, no
     * action occurs and false is returned immediately. immediately;
     *
     * @param newCount to which to update {@code count}; must be non-negative.
     * @return {@code true} if {@code count} transitions to zero.
     * @throws IllegalArgumentException when {@code newCount} is negative
     */
    public boolean setCount(int newCount) {
        return sync.setCount(newCount);
    }

    /**
     * Returns a string representation of this object.
     * <p>
     *
     * @return a string identifying this latch, as well as its current
     * {@code count}.
     */
    public String toString() {
        return String.format("%s[count=%d]", super.toString(),
                sync.getCount());
    }

    /**
     * As much as is reasonably practical, returns distinct integers for
     * distinct objects.
     *
     * @return a hash code value for this latch. This method is supported for
     * the benefit of hash tables
     */

    //@Override
    public int hashCode() {
        return super.hashCode();
    }

    /**
     * Returns true if and only if {@code this} and {@code obj} refer to the
     * same object ({@code this == obj} has the value {@code true}).
     *
     * @param other to test.
     * @return if and only if {@code this == obj}
     */
    public boolean equals(final CountUpDownLatch other) {
        return this == other;
    }

    /**
     * Returns true if and only if {@code this} and {@code obj} refer to the
     * same object ({@code this == obj} has the value {@code true}).
     *
     * @param obj to test.
     * @return if and only if {@code this == obj}
     */

    //@Override
    //@SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
    public boolean equals(Object obj) {
        return this == obj;
    }

    /**
     * Synchronization control For {@link CountUpDownLatch}.
     * <p>
     * Uses {@link AbstractQueuedSynchronizer} {@code state} property to
     * represent count.
     */
    private static class Sync extends AbstractQueuedSynchronizer {

        private static final long serialVersionUID = 7224851200740908493L;

        Sync(int count) {
            setState(count);
        }

        //@Override
        protected boolean tryReleaseShared(int arg) {
            return arg == 0;
        }

        int getCount() {
            return getState();
        }

        /**
         * Queries if the state of this synchronizer permits it to be acquired
         * in the shared mode, and if so to acquire it.
         * <p>
         * This implementation supports the required semantics of the
         * {@code await(...)} methods of the enclosing {@link CountUpDownLatch}
         * class.
         *
         * @param ignored
         * @return -1 on failure; 1 if acquisition in shared mode succeeded and
         * subsequent shared-mode acquires might also succeed, in which
         * case a subsequent waiting thread must check availability.
         */
        protected int tryAcquireShared(int ignored) {
            return getState() == 0 ? 1
                    : -1;
        }

        /**
         * Updates {@code count} to {@code newCount}, returning {@code true} on
         * transition to zero.
         * <p>
         * If {@code newCount} is zero and the current {@code count} is zero, no
         * action occurs and false is returned immediately. immediately;
         *
         * @param newCount to which to update {@code count}; must be
         *                 non-negative.
         * @return {@code true} if {@code count} transitions to zero.
         * @throws IllegalArgumentException when {@code newCount} is negative
         */
        boolean setCount(int newCount) {

            if (newCount < 0) {
                throw new IllegalArgumentException(
                        String.format(
                                "amount must be non-negative: %d", newCount));
            }

            boolean requestedZero = newCount == 0;

            for (; ; ) {
                int c = getState();

                if (requestedZero && c == 0) {
                    return false;
                }

                // assert newCount >= 0;
                if (compareAndSetState(c, newCount)) {
                    return requestedZero ? releaseShared(0)
                            : false;
                }
            }
        }

        /**
         * by one.
         *
         * @return true if {#code count} transitioned to zero.
         */
        boolean countDown() {

            for (; ; ) {
                int c = getState();

                if (c == 0) {
                    return false;
                }

                // assert: nextc >= 0;
                int nextc = c - 1;

                if (super.compareAndSetState(c, nextc)) {
                    if (nextc == 0) {
                        return releaseShared(0);
                    } else {
                        return false;
                    }
                }
            }
        }

        /**
         * by {@code amount}.
         *
         * @param amount by which to decrement the {@code count}
         * @return true if {#code count} transitioned to zero.
         * @throws IllegalArgumentException when {@code amount} is non-positive
         */
        boolean countDown(int amount) {

            if (amount < 1) {
                throw new IllegalArgumentException(
                        String.format("Amount must be positive: %d", amount));
            }

            for (; ; ) {
                int c = getState();

                if (c == 0) {
                    return false;
                }

                int nextc = amount >= c ? 0
                        : c - amount;

                // assert nextc >= 0;
                if (super.compareAndSetState(c, nextc)) {
                    return nextc == 0 ? releaseShared(0)
                            : false;
                }
            }
        }

        /**
         * by one.
         *
         * @return true if count transitioned to zero
         * @throws ArithmeticException when the operation would otherwise cause
         *                             a silent numeric overflow, resulting in a
         *                             negative {@code count}.
         */
        boolean countUp() {

            for (; ; ) {
                int c = getState();

                if (c == Integer.MAX_VALUE) {
                    throw new ArithmeticException(
                            String.format("integer overflow: %d + 1", c));
                }

                int nextc = c + 1;

                // assert: nextc >= 0;
                if (super.compareAndSetState(c, nextc)) {
                    return c == 0;
                }
            }
        }

        /**
         * by {@code amount}.
         *
         * @param amount by which to increment {@code count}
         * @return true if count transitioned from zero to a new value
         * @throws ArithmeticException      when the operation would otherwise
         *                                  cause a silent numeric overflow,
         *                                  resulting in a negative
         *                                  {@code count}.
         * @throws IllegalArgumentException if {@code amount is less than one)
         */
        boolean countUp(int amount) {

            if (amount < 1) {
                throw new IllegalArgumentException(
                        String.format("amount must be positive: %d", amount));
            }

            for (; ; ) {
                int c = getState();

                if (amount > Integer.MAX_VALUE - c) {
                    throw new ArithmeticException(
                            String.format("integer overflow: %d + $d", c, amount));
                }

                int nextc = c + amount;

                // assert: nextc >= 0;
                if (super.compareAndSetState(c, nextc)) {
                    return c == 0;
                }
            }
        }
    }
}

