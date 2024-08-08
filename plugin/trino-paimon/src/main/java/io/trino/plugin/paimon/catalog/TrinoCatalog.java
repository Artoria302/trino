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
package io.trino.plugin.paimon.catalog;

import io.trino.spi.security.TrinoPrincipal;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.fs.Path;
import org.apache.paimon.schema.Schema;

import java.util.Map;
import java.util.Optional;

public interface TrinoCatalog
{
    void createTable(Identifier identifier, Schema schema, String owner, Optional<Path> location, boolean ignoreIfExists)
            throws Catalog.TableAlreadyExistException, Catalog.DatabaseNotExistException;

    void createDatabase(String name, boolean ignoreIfExists, Map<String, String> properties, TrinoPrincipal owner)
            throws Catalog.DatabaseAlreadyExistException;
}
