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

import io.trino.plugin.base.classloader.ClassLoaderSafeConnectorMetadata;
import io.trino.plugin.hive.HiveTransactionHandle;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.session.PropertyMetadata;
import io.trino.spi.transaction.IsolationLevel;

import java.util.List;

import static io.trino.spi.transaction.IsolationLevel.READ_COMMITTED;
import static io.trino.spi.transaction.IsolationLevel.checkConnectorSupports;
import static java.util.Objects.requireNonNull;

/**
 * Trino {@link Connector}.
 */
public class PaimonConnector
        implements Connector
{
    private final PaimonTransactionManager transactionManager;
    private final ConnectorSplitManager trinoSplitManager;
    private final ConnectorPageSourceProvider trinoPageSourceProvider;
    private final List<PropertyMetadata<?>> tableProperties;
    private final List<PropertyMetadata<?>> sessionProperties;

    public PaimonConnector(
            PaimonTransactionManager transactionManager,
            ConnectorSplitManager trinoSplitManager,
            ConnectorPageSourceProvider trinoPageSourceProvider,
            PaimonTableOptions paimonTableOptions,
            PaimonSessionProperties paimonSessionProperties)
    {
        this.transactionManager = requireNonNull(transactionManager, "transactionManager is null");
        this.trinoSplitManager = requireNonNull(trinoSplitManager, "trinoSplitManager is null");
        this.trinoPageSourceProvider = requireNonNull(trinoPageSourceProvider, "trinoRecordSetProvider is null");
        this.tableProperties = paimonTableOptions.getTableOptions();
        this.sessionProperties = paimonSessionProperties.getSessionProperties();
    }

    @Override
    public ConnectorTransactionHandle beginTransaction(IsolationLevel isolationLevel, boolean readOnly, boolean autoCommit)
    {
        checkConnectorSupports(READ_COMMITTED, isolationLevel);
        ConnectorTransactionHandle transaction = new HiveTransactionHandle(autoCommit);
        transactionManager.begin(transaction);
        return transaction;
    }

    @Override
    public void commit(ConnectorTransactionHandle transaction)
    {
        transactionManager.commit(transaction);
    }

    @Override
    public void rollback(ConnectorTransactionHandle transaction)
    {
        transactionManager.rollback(transaction);
    }

    @Override
    public ConnectorMetadata getMetadata(ConnectorSession session, ConnectorTransactionHandle transaction)
    {
        ConnectorMetadata metadata = transactionManager.get(transaction, session);
        return new ClassLoaderSafeConnectorMetadata(metadata, getClass().getClassLoader());
    }

    @Override
    public ConnectorSplitManager getSplitManager()
    {
        return trinoSplitManager;
    }

    @Override
    public ConnectorPageSourceProvider getPageSourceProvider()
    {
        return trinoPageSourceProvider;
    }

    @Override
    public List<PropertyMetadata<?>> getSessionProperties()
    {
        return sessionProperties;
    }

    @Override
    public List<PropertyMetadata<?>> getTableProperties()
    {
        return tableProperties;
    }
}
