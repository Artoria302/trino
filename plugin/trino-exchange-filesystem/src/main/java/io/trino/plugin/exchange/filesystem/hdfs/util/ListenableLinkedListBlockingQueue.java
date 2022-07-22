/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.exchange.filesystem.hdfs.util;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;

import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import static java.util.Objects.requireNonNull;

public class ListenableLinkedListBlockingQueue<E>
{
    private final int capacity;
    private int count;
    private final Queue<E> elements = new LinkedList<>();
    private SettableFuture<Void> notFullFuture = SettableFuture.create();
    private boolean someoneWaitingOnCurrentNotFullFuture;
    private SettableFuture<Void> notEmptyFuture = SettableFuture.create();
    private boolean someoneWaitingOnCurrentNotEmptyFuture;
    private final ReentrantLock lock = new ReentrantLock();
    private final Condition notEmpty = lock.newCondition();
    private final Condition notFull = lock.newCondition();

    private final Executor executor;

    public ListenableLinkedListBlockingQueue(int capacity, Executor executor)
    {
        this.capacity = capacity;
        this.executor = executor;
    }

    public ListenableLinkedListBlockingQueue(Executor executor)
    {
        this.capacity = Integer.MAX_VALUE;
        this.executor = executor;
    }

    public int getCapacity()
    {
        return capacity;
    }

    private void enqueue(E e)
    {
        Queue<E> elements = this.elements;
        elements.add(e);
        count++;
        notEmpty.signal();
        if (someoneWaitingOnCurrentNotEmptyFuture) {
            completeAsync(executor, notEmptyFuture);
            notEmptyFuture = SettableFuture.create();
            someoneWaitingOnCurrentNotEmptyFuture = false;
        }
    }

    private EnqueueStatus enqueueListenable(E e, boolean enqueue)
    {
        if (enqueue) {
            enqueue(e);
            return new EnqueueStatus(true, null);
        }
        else {
            someoneWaitingOnCurrentNotFullFuture = true;
            return new EnqueueStatus(false, notFullFuture);
        }
    }

    public boolean offer(E e)
    {
        requireNonNull(e);
        final ReentrantLock lock = this.lock;
        lock.lock();
        try {
            if (count == capacity) {
                return false;
            }
            else {
                enqueue(e);
                return true;
            }
        }
        finally {
            lock.unlock();
        }
    }

    public EnqueueStatus offerListenable(E e)
    {
        requireNonNull(e);
        final ReentrantLock lock = this.lock;
        lock.lock();
        try {
            return enqueueListenable(e, count != capacity);
        }
        finally {
            lock.unlock();
        }
    }

    public void put(E e)
            throws InterruptedException
    {
        requireNonNull(e);
        final ReentrantLock lock = this.lock;
        lock.lockInterruptibly();
        try {
            while (count == capacity) {
                notFull.await();
            }
            enqueue(e);
        }
        finally {
            lock.unlock();
        }
    }

    public boolean offer(E e, long timeout, TimeUnit unit)
            throws InterruptedException
    {
        requireNonNull(e);
        long nanos = unit.toNanos(timeout);
        final ReentrantLock lock = this.lock;
        lock.lockInterruptibly();
        try {
            while (count == capacity) {
                if (nanos <= 0L) {
                    return false;
                }
                nanos = notFull.awaitNanos(nanos);
            }
            enqueue(e);
            return true;
        }
        finally {
            lock.unlock();
        }
    }

    public EnqueueStatus offerListenable(E e, long timeout, TimeUnit unit)
            throws InterruptedException
    {
        requireNonNull(e);
        long nanos = unit.toNanos(timeout);
        final ReentrantLock lock = this.lock;
        lock.lockInterruptibly();
        try {
            while (count == capacity) {
                if (nanos <= 0L) {
                    return enqueueListenable(e, false);
                }
                nanos = notFull.awaitNanos(nanos);
            }
            return enqueueListenable(e, true);
        }
        finally {
            lock.unlock();
        }
    }

    private E dequeue()
    {
        Queue<E> elements = this.elements;
        E e = elements.poll();
        count--;
        notFull.signal();
        if (someoneWaitingOnCurrentNotFullFuture) {
            completeAsync(executor, notFullFuture);
            notFullFuture = SettableFuture.create();
            someoneWaitingOnCurrentNotFullFuture = false;
        }
        return e;
    }

    private DequeueStatus<E> dequeueListenable(boolean dequeue)
    {
        if (dequeue) {
            E e = dequeue();
            return new DequeueStatus<>(e, null);
        }
        else {
            someoneWaitingOnCurrentNotEmptyFuture = true;
            return new DequeueStatus<>(null, notEmptyFuture);
        }
    }

    public E poll()
    {
        final ReentrantLock lock = this.lock;
        lock.lock();
        try {
            return (count == 0) ? null : dequeue();
        }
        finally {
            lock.unlock();
        }
    }

    public DequeueStatus<E> pollListenable()
    {
        final ReentrantLock lock = this.lock;
        lock.lock();
        try {
            return dequeueListenable(count != 0);
        }
        finally {
            lock.unlock();
        }
    }

    public E take()
            throws InterruptedException
    {
        final ReentrantLock lock = this.lock;
        lock.lockInterruptibly();
        try {
            while (count == 0) {
                notEmpty.await();
            }
            return dequeue();
        }
        finally {
            lock.unlock();
        }
    }

    public DequeueStatus<E> pollListenable(long timeout, TimeUnit unit)
            throws InterruptedException
    {
        long nanos = unit.toNanos(timeout);
        final ReentrantLock lock = this.lock;
        lock.lockInterruptibly();
        try {
            while (count == 0) {
                if (nanos <= 0L) {
                    return dequeueListenable(false);
                }
                nanos = notEmpty.awaitNanos(nanos);
            }
            return dequeueListenable(true);
        }
        finally {
            lock.unlock();
        }
    }

    public boolean signalWaiting()
    {
        boolean someoneWaiting = false;
        final ReentrantLock lock = this.lock;
        lock.lock();
        try {
            if (someoneWaitingOnCurrentNotEmptyFuture) {
                someoneWaiting = true;
                completeAsync(executor, notEmptyFuture);
                notEmptyFuture = SettableFuture.create();
                someoneWaitingOnCurrentNotEmptyFuture = false;
            }
            if (someoneWaitingOnCurrentNotFullFuture) {
                someoneWaiting = true;
                completeAsync(executor, notFullFuture);
                notFullFuture = SettableFuture.create();
                someoneWaitingOnCurrentNotFullFuture = false;
            }
        }
        finally {
            lock.unlock();
        }
        return someoneWaiting;
    }

    private static void completeAsync(Executor executor, SettableFuture<Void> future)
    {
        executor.execute(() -> future.set(null));
    }

    public static class DequeueStatus<E>
    {
        private final E element;
        private final ListenableFuture<Void> listenableFuture;

        DequeueStatus(E element, ListenableFuture<Void> listenableFuture)
        {
            this.element = element;
            this.listenableFuture = listenableFuture;
        }

        public E getElement()
        {
            return element;
        }

        public ListenableFuture<Void> getListenableFuture()
        {
            return listenableFuture;
        }
    }

    static class EnqueueStatus
    {
        private final boolean enqueued;
        private final ListenableFuture<Void> listenableFuture;

        EnqueueStatus(boolean enqueued, ListenableFuture<Void> listenableFuture)
        {
            this.enqueued = enqueued;
            this.listenableFuture = listenableFuture;
        }

        public ListenableFuture<Void> getListenableFuture()
        {
            return listenableFuture;
        }

        public boolean isEnqueued()
        {
            return enqueued;
        }
    }
}
