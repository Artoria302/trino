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

import io.airlift.slice.Slice;
import io.trino.spi.type.Type;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

/**
 * @param type Column type
 * @param miss Whether column is missed from parquet file (row position and inverted index query result can be read as a metadata column from native library)
 * @param invertedIndexQuery query string to fill
 */
public record ParquetReaderColumn(Type type, boolean miss, Optional<Slice> invertedIndexQuery)
{
    public ParquetReaderColumn
    {
        requireNonNull(type, "type is null");
    }
}
