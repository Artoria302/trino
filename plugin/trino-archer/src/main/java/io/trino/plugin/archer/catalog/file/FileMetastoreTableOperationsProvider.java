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
package io.trino.plugin.archer.catalog.file;

import com.google.inject.Inject;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.plugin.archer.catalog.ArcherTableOperations;
import io.trino.plugin.archer.catalog.ArcherTableOperationsProvider;
import io.trino.plugin.archer.catalog.TrinoCatalog;
import io.trino.plugin.archer.catalog.hms.TrinoHiveCatalog;
import io.trino.plugin.archer.fileio.ForwardingFileIo;
import io.trino.spi.connector.ConnectorSession;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class FileMetastoreTableOperationsProvider
        implements ArcherTableOperationsProvider
{
    private final TrinoFileSystemFactory fileSystemFactory;

    @Inject
    public FileMetastoreTableOperationsProvider(TrinoFileSystemFactory fileSystemFactory)
    {
        this.fileSystemFactory = requireNonNull(fileSystemFactory, "fileSystemFactory is null");
    }

    @Override
    public ArcherTableOperations createTableOperations(
            TrinoCatalog catalog,
            ConnectorSession session,
            String database,
            String table,
            Optional<String> owner,
            Optional<String> location)
    {
        return new FileMetastoreTableOperations(
                new ForwardingFileIo(fileSystemFactory.create(session)),
                ((TrinoHiveCatalog) catalog).getMetastore(),
                session,
                database,
                table,
                owner,
                location);
    }
}
