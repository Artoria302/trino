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
package io.trino.resourcemanager;

import com.google.common.annotations.VisibleForTesting;
import io.trino.metadata.InternalNode;
import io.trino.metadata.InternalNodeManager;

import javax.inject.Inject;

import java.net.URI;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Function;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public class RandomResourceManagerAddressSelector
        implements ResourceManagerAddressSelector
{
    private final InternalNodeManager internalNodeManager;
    private final Function<List<URI>, Optional<URI>> hostSelector;

    @Inject
    public RandomResourceManagerAddressSelector(InternalNodeManager internalNodeManager)
    {
        this(internalNodeManager, RandomResourceManagerAddressSelector::selectRandomHost);
    }

    @VisibleForTesting
    RandomResourceManagerAddressSelector(
            InternalNodeManager internalNodeManager,
            Function<List<URI>, Optional<URI>> hostSelector)
    {
        this.internalNodeManager = requireNonNull(internalNodeManager, "internalNodeManager is null");
        this.hostSelector = requireNonNull(hostSelector, "hostSelector is null");
    }

    @Override
    public Optional<URI> selectAddress()
    {
        List<URI> resourceManagers = internalNodeManager.getResourceManagers().stream()
                .map(InternalNode::getInternalUri)
                .collect(toImmutableList());
        return hostSelector.apply(resourceManagers);
    }

    private static Optional<URI> selectRandomHost(List<URI> uris)
    {
        if (uris.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(uris.get(ThreadLocalRandom.current().nextInt(uris.size())));
    }
}
