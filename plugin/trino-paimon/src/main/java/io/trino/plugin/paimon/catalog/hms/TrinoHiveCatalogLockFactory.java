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

import io.trino.plugin.hive.metastore.thrift.ThriftMetastore;
import io.trino.spi.connector.ConnectorSession;
import org.apache.paimon.catalog.CatalogLock;
import org.apache.paimon.catalog.CatalogLockContext;
import org.apache.paimon.catalog.CatalogLockFactory;

import static java.util.Objects.requireNonNull;

public class TrinoHiveCatalogLockFactory
        implements CatalogLockFactory
{
    private final ThriftMetastore thriftMetastore;
    private final ConnectorSession session;

    public TrinoHiveCatalogLockFactory(ThriftMetastore thriftMetastore, ConnectorSession session)
    {
        this.thriftMetastore = requireNonNull(thriftMetastore, "thriftMetastore is null");
        this.session = requireNonNull(session, "session is null");
    }

    @Override
    public CatalogLock createLock(CatalogLockContext context)
    {
        return new TrinoHiveCatalogLock(thriftMetastore, session);
    }

    @Override
    public String identifier()
    {
        return "hive";
    }
}
