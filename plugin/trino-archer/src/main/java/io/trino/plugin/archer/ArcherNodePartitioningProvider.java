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
package io.trino.plugin.archer;

import com.google.inject.Inject;
import io.trino.spi.NodeManager;
import io.trino.spi.connector.BucketFunction;
import io.trino.spi.connector.ConnectorBucketNodeMap;
import io.trino.spi.connector.ConnectorNodePartitioningProvider;
import io.trino.spi.connector.ConnectorPartitioningHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;
import io.trino.spi.type.TypeOperators;
import net.qihoo.archer.Schema;

import java.util.List;
import java.util.Optional;

import static io.trino.plugin.archer.ArcherSessionProperties.getPartitionedBucketsPerNode;
import static io.trino.plugin.archer.ArcherUtil.schemaFromHandles;
import static io.trino.plugin.archer.PartitionFields.parsePartitionFields;
import static io.trino.spi.connector.ConnectorBucketNodeMap.createBucketNodeMap;
import static java.util.Objects.requireNonNull;

public class ArcherNodePartitioningProvider
        implements ConnectorNodePartitioningProvider
{
    private final NodeManager nodeManager;
    private final TypeOperators typeOperators;

    @Inject
    public ArcherNodePartitioningProvider(NodeManager nodeManager, TypeManager typeManager)
    {
        this.nodeManager = requireNonNull(nodeManager, "nodeManager is null");
        this.typeOperators = typeManager.getTypeOperators();
    }

    @Override
    public BucketFunction getBucketFunction(
            ConnectorTransactionHandle transactionHandle,
            ConnectorSession session,
            ConnectorPartitioningHandle partitioningHandle,
            List<Type> partitionChannelTypes,
            int bucketCount)
    {
        if (partitioningHandle instanceof ArcherUpdateHandle) {
            // used for merge
            return new ArcherUpdateBucketFunction(bucketCount);
        }

        ArcherPartitioningHandle handle = (ArcherPartitioningHandle) partitioningHandle;
        Schema schema = schemaFromHandles(handle.getPartitioningColumns());
        return new ArcherBucketFunction(
                typeOperators,
                parsePartitionFields(schema, handle.getPartitioning()),
                handle.getPartitioningColumns(),
                bucketCount);
    }

    @Override
    public Optional<ConnectorBucketNodeMap> getBucketNodeMapping(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorPartitioningHandle partitioningHandle)
    {
        int partitionedBucketsPerNode = getPartitionedBucketsPerNode(session);
        if (partitionedBucketsPerNode > 0) {
            return Optional.of(createBucketNodeMap(nodeManager.getRequiredWorkerNodes().size() * partitionedBucketsPerNode));
        }
        else {
            return Optional.empty();
        }
    }
}
