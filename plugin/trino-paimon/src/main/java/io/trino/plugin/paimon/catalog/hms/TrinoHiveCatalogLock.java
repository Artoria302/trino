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

import io.airlift.log.Logger;
import io.trino.metastore.AcidTransactionOwner;
import io.trino.plugin.hive.metastore.thrift.ThriftMetastore;
import io.trino.spi.connector.ConnectorSession;
import org.apache.paimon.catalog.CatalogLock;

import java.io.IOException;
import java.util.concurrent.Callable;

import static java.util.Objects.requireNonNull;

public class TrinoHiveCatalogLock
        implements CatalogLock
{
    private static final Logger LOG = Logger.get(TrinoHiveCatalogLock.class);

    private final ThriftMetastore thriftMetastore;
    private final ConnectorSession session;

    public TrinoHiveCatalogLock(ThriftMetastore thriftMetastore, ConnectorSession session)
    {
        this.thriftMetastore = requireNonNull(thriftMetastore, "thriftMetastore is null");
        this.session = requireNonNull(session, "session is null");
    }

    @Override
    public <T> T runWithLock(String database, String table, Callable<T> callable)
            throws Exception
    {
        long lockId = thriftMetastore.acquireTableExclusiveLock(
                new AcidTransactionOwner(session.getUser()),
                session.getQueryId(),
                database,
                table);
        try {
            return callable.call();
        }
        finally {
            try {
                thriftMetastore.releaseTableLock(lockId);
            }
            catch (RuntimeException e) {
                // Release lock step has failed. Not throwing this exception, after commit has already succeeded.
                // So, that underlying iceberg API will not do the metadata cleanup, otherwise table will be in unusable state.
                // If configured and supported, the unreleased lock will be automatically released by the metastore after not hearing a heartbeat for a while,
                // or otherwise it might need to be manually deleted from the metastore backend storage.
                LOG.error(e, "Failed to release lock %s when committing to table %s.%s", lockId, database, table);
            }
        }
    }

    @Override
    public void close()
            throws IOException
    {
    }
}
