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
package io.trino.plugin.paimon;

import com.google.inject.Inject;
import io.trino.filesystem.cache.CachingHostAddressProvider;
import io.trino.plugin.paimon.catalog.PaimonCatalogFactory;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.DynamicFilter;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.Split;

import java.util.List;
import java.util.stream.Collectors;

import static io.trino.plugin.paimon.PaimonSessionProperties.getCacheNodeCount;
import static io.trino.plugin.paimon.PaimonSessionProperties.getMinimumSplitWeight;
import static io.trino.plugin.paimon.PaimonSessionProperties.isLocalCacheEnabled;
import static java.util.Objects.requireNonNull;

/**
 * Trino {@link ConnectorSplitManager}.
 */
public class PaimonSplitManager
        implements ConnectorSplitManager
{
    private final PaimonCatalogFactory paimonCatalogFactory;
    private final CachingHostAddressProvider cachingHostAddressProvider;

    @Inject
    public PaimonSplitManager(PaimonCatalogFactory paimonCatalogFactory, CachingHostAddressProvider cachingHostAddressProvider)
    {
        this.paimonCatalogFactory = requireNonNull(paimonCatalogFactory, "paimonCatalogFactory is null");
        this.cachingHostAddressProvider = requireNonNull(cachingHostAddressProvider, "cachingHostAddressProvider is null");
    }

    @Override
    public ConnectorSplitSource getSplits(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorTableHandle table,
            DynamicFilter dynamicFilter,
            Constraint constraint)
    {
        return getSplits(table, session);
    }

    protected ConnectorSplitSource getSplits(ConnectorTableHandle connectorTableHandle, ConnectorSession session)
    {
        // TODO dynamicFilter?
        // TODO what is constraint?

        PaimonTableHandle tableHandle = (PaimonTableHandle) connectorTableHandle;
        Catalog catalog = paimonCatalogFactory.create(session);
        Table table = tableHandle.tableWithDynamicOptions(catalog, session);
        ReadBuilder readBuilder = table.newReadBuilder();
        new PaimonFilterConverter(table.rowType())
                .convert(tableHandle.getFilter())
                .ifPresent(readBuilder::withFilter);
        tableHandle.getLimit().ifPresent(limit -> readBuilder.withLimit((int) limit));
        List<Split> splits = readBuilder.newScan().plan().splits();

        boolean localCacheEnabled = isLocalCacheEnabled(session);
        int cacheNodeCount = getCacheNodeCount(session);

        long maxRowCount = splits.stream().mapToLong(Split::rowCount).max().orElse(0L);
        double minimumSplitWeight = getMinimumSplitWeight(session);
        return new PaimonSplitSource(
                splits.stream()
                        .map(split -> PaimonSplit.fromSplit(
                                split,
                                maxRowCount == 0 ? minimumSplitWeight : Math.min(Math.max((double) split.rowCount() / maxRowCount, minimumSplitWeight), 1.0),
                                cachingHostAddressProvider,
                                localCacheEnabled,
                                cacheNodeCount))
                        .collect(Collectors.toList()),
                ((PaimonTableHandle) connectorTableHandle).getLimit());
    }
}
