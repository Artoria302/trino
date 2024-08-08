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

import io.trino.spi.connector.ConnectorSession;
import org.apache.paimon.catalog.AbstractCatalog;

import java.util.Locale;

public interface PaimonCatalogFactory
{
    AbstractCatalog create(ConnectorSession session);

    default boolean checkObjectStore(String scheme)
    {
        scheme = scheme.toLowerCase(Locale.ENGLISH);
        if (!scheme.startsWith("s3")
                && !scheme.startsWith("emr")
                && !scheme.startsWith("oss")
                && !scheme.startsWith("wasb")) {
            return scheme.startsWith("http") || scheme.startsWith("ftp");
        }
        else {
            return true;
        }
    }
}
