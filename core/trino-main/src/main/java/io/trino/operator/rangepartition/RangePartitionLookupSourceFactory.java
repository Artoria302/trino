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

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.trino.spi.type.Type;

import javax.annotation.concurrent.GuardedBy;

import java.util.List;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static com.google.common.util.concurrent.Futures.immediateVoidFuture;
import static com.google.common.util.concurrent.Futures.nonCancellationPropagating;
import static com.google.common.util.concurrent.Futures.transform;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static java.util.Objects.requireNonNull;

public class RangePartitionLookupSourceFactory
        implements LookupBridge
{
    private final List<Type> types;
    @GuardedBy("this")
    private final SettableFuture<Void> lookupSourceNoLongerNeeded = SettableFuture.create();
    @GuardedBy("this")
    private final SettableFuture<Void> destroyed = SettableFuture.create();
    @GuardedBy("this")
    private final SettableFuture<LookupSource> lookupSourceFuture = SettableFuture.create();
    @GuardedBy("this")
    private LookupSource lookupSource;

    public RangePartitionLookupSourceFactory(List<Type> types)
    {
        this.types = ImmutableList.copyOf(requireNonNull(types, "types is null"));
    }

    public synchronized ListenableFuture<LookupSource> createLookupSource()
    {
        checkState(!destroyed.isDone(), "already destroyed");
        if (lookupSource != null) {
            return immediateFuture(lookupSource);
        }

        return lookupSourceFuture;
    }

    @Override
    public ListenableFuture<Void> whenBuildFinishes()
    {
        return transform(
                this.createLookupSource(),
                lookupSourceProvider -> {
                    // Close the lookupSourceProvider we just created.
                    // The only reason we created it is to wait until lookup source is ready.
                    lookupSourceProvider.close();
                    return null;
                },
                directExecutor());
    }

    public List<Type> getTypes()
    {
        return types;
    }

    public ListenableFuture<Void> lendLookupSource(LookupSource lookupSource)
    {
        SettableFuture<LookupSource> lookupSourceFuture;
        synchronized (this) {
            if (destroyed.isDone()) {
                return immediateVoidFuture();
            }

            checkState(this.lookupSource == null, "lookupSourceSupplier already set");
            this.lookupSource = lookupSource;
            lookupSourceFuture = this.lookupSourceFuture;
        }

        lookupSourceFuture.set(lookupSource);

        return lookupSourceNoLongerNeeded;
    }

    @Override
    public synchronized void destroy()
    {
        free();

        // Setting destroyed must be last because it's a part of the state exposed by isDestroyed() without synchronization.
        destroyed.set(null);
    }

    private void free()
    {
        // Let the RangeIndexBuilderOperator reduce their accounted memory
        lookupSourceNoLongerNeeded.set(null);
        synchronized (this) {
            lookupSource = null;
        }
    }

    @SuppressWarnings("FieldAccessNotGuarded")
    public ListenableFuture<Void> isDestroyed()
    {
        return nonCancellationPropagating(destroyed);
    }
}
