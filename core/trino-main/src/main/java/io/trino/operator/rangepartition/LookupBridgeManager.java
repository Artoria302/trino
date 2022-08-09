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
package io.trino.operator.rangepartition;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.trino.operator.ReferenceCount;

import java.util.concurrent.atomic.AtomicBoolean;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static java.util.Objects.requireNonNull;

public class LookupBridgeManager<T extends LookupBridge>
{
    private final AtomicBoolean initialized = new AtomicBoolean();
    private final T lookupBridge;
    private final FreezeOnReadCounter lookupFactoryCount = new FreezeOnReadCounter();

    private LookupLifecycle lookupLifecycle;

    public LookupBridgeManager(
            T lookupBridge)
    {
        this.lookupBridge = requireNonNull(lookupBridge, "lookupBridge is null");
    }

    private void initializeIfNecessary()
    {
        if (!initialized.get()) {
            synchronized (this) {
                if (initialized.get()) {
                    return;
                }
                int finalLookupFactoryCount = lookupFactoryCount.get();
                lookupLifecycle = new LookupLifecycle(lookupBridge, finalLookupFactoryCount);
                initialized.set(true);
            }
        }
    }

    public T getLookupBridge()
    {
        initializeIfNecessary();
        return lookupBridge;
    }

    public void incrementLookupFactoryCount()
    {
        lookupFactoryCount.increment();
    }

    public void lookupOperatorFactoryClosed()
    {
        initializeIfNecessary();
        lookupLifecycle.releaseForLookup();
    }

    public void lookupOperatorCreated()
    {
        initializeIfNecessary();
        lookupLifecycle.retainForLookup();
    }

    public void lookupOperatorClosed()
    {
        initializeIfNecessary();
        lookupLifecycle.releaseForLookup();
    }

    private static class LookupLifecycle
    {
        private final ReferenceCount lookupReferenceCount;
        private final ListenableFuture<Void> whenBuildAndLookupFinishes;

        public LookupLifecycle(LookupBridge lookupBridge, int lookupFactoryCount)
        {
            // When all lookup operators finish, destroy the lookup bridge (freeing the memory)
            // * Each lookup operator factory count as 1
            // * Each lookup operator count as 1
            lookupReferenceCount = new ReferenceCount(lookupFactoryCount);

            whenBuildAndLookupFinishes = Futures.whenAllSucceed(lookupBridge.whenBuildFinishes(), lookupReferenceCount.getFreeFuture()).call(() -> null, directExecutor());
            whenBuildAndLookupFinishes.addListener(lookupBridge::destroy, directExecutor());
        }

        public ListenableFuture<Void> whenBuildAndLookupFinishes()
        {
            return whenBuildAndLookupFinishes;
        }

        private void retainForLookup()
        {
            lookupReferenceCount.retain();
        }

        private void releaseForLookup()
        {
            lookupReferenceCount.release();
        }
    }

    private static class FreezeOnReadCounter
    {
        private int count;
        private boolean frozen;

        public synchronized void increment()
        {
            checkState(!frozen, "Counter has been read");
            count++;
        }

        public synchronized int get()
        {
            frozen = true;
            return count;
        }
    }
}
