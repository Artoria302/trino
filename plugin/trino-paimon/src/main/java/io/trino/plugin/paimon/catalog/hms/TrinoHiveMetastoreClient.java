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
package io.trino.plugin.paimon.catalog.hms;

import io.trino.metastore.Column;
import io.trino.metastore.HiveMetastore;
import io.trino.metastore.Partition;
import io.trino.metastore.PartitionStatistics;
import io.trino.metastore.PartitionWithStatistics;
import io.trino.metastore.Table;
import io.trino.plugin.hive.PartitionNotFoundException;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.metastore.MetastoreClient;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.utils.InternalRowPartitionComputer;
import org.apache.paimon.utils.PartitionPathUtils;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.hive.util.HiveUtil.makePartName;
import static java.util.Objects.requireNonNull;

public class TrinoHiveMetastoreClient
        implements MetastoreClient
{
    private final HiveMetastore metastore;
    private final Identifier identifier;
    private final InternalRowPartitionComputer partitionComputer;

    public TrinoHiveMetastoreClient(HiveMetastore metastore, Identifier identifier, TableSchema schema)
    {
        this.metastore = requireNonNull(metastore, "metastore is null");
        this.identifier = requireNonNull(identifier, "identifier");
        this.partitionComputer =
                new InternalRowPartitionComputer(
                        new CoreOptions(schema.options()).partitionDefaultName(),
                        schema.logicalPartitionType(),
                        schema.partitionKeys().toArray(new String[0]));
    }

    @Override
    public void addPartition(BinaryRow partition)
            throws Exception
    {
        addPartition(partitionComputer.generatePartValues(partition));
    }

    @Override
    public void addPartition(LinkedHashMap<String, String> partitionSpec)
            throws Exception
    {
        List<String> partitionValues = new ArrayList<>(partitionSpec.values());
        Optional<Table> tableOptional = metastore.getTable(identifier.getDatabaseName(), identifier.getObjectName());
        if (tableOptional.isEmpty()) {
            throw new Catalog.TableNotExistException(identifier);
        }
        Table table = tableOptional.get();
        Optional<Partition> partitionOptional = metastore.getPartition(table, partitionValues);
        if (partitionOptional.isPresent()) {
            return;
        }
        Partition partition = Partition.builder()
                .setDatabaseName(table.getDatabaseName())
                .setTableName(table.getTableName())
                .setColumns(table.getDataColumns())
                .setValues(partitionValues)
                .withStorage(storage -> storage
                        .setStorageFormat(table.getStorage().getStorageFormat())
                        .setLocation(table.getStorage().getLocation() + "/" + PartitionPathUtils.generatePartitionPath(partitionSpec))
                        .setBucketProperty(table.getStorage().getBucketProperty())
                        .setSerdeParameters(table.getStorage().getSerdeParameters()))
                .build();
        String partitionName = getPartitionName(table, partition.getValues());
        PartitionWithStatistics partitionWithStatistics = new PartitionWithStatistics(partition, partitionName, PartitionStatistics.empty());
        metastore.addPartitions(identifier.getDatabaseName(), identifier.getObjectName(), List.of(partitionWithStatistics));
    }

    private static String getPartitionName(Table table, List<String> partitionValues)
    {
        List<String> columnNames = table.getPartitionColumns().stream()
                .map(Column::getName)
                .collect(toImmutableList());
        return makePartName(columnNames, partitionValues);
    }

    @Override
    public void deletePartition(LinkedHashMap<String, String> partitionSpec)
            throws Exception
    {
        List<String> partitionValues = new ArrayList<>(partitionSpec.values());
        try {
            metastore.dropPartition(identifier.getDatabaseName(), identifier.getObjectName(), partitionValues, false);
        }
        catch (PartitionNotFoundException ignore) {
        }
        catch (RuntimeException ex) {
            throw new Exception(ex);
        }
    }

    @Override
    public void close()
            throws Exception
    {
    }

    public static class Factory
            implements MetastoreClient.Factory
    {
        private final HiveMetastore metastore;
        private final Identifier identifier;
        private final TableSchema schema;

        public Factory(HiveMetastore metastore, Identifier identifier, TableSchema schema)
        {
            this.metastore = requireNonNull(metastore, "metastore is null");
            this.identifier = requireNonNull(identifier, "identifier");
            this.schema = requireNonNull(schema, "schema is null");
        }

        @Override
        public MetastoreClient create()
        {
            return new TrinoHiveMetastoreClient(metastore, identifier, schema);
        }
    }
}
