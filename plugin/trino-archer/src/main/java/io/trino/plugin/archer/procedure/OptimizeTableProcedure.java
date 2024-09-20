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
package io.trino.plugin.archer.procedure;

import com.google.common.collect.ImmutableList;
import com.google.inject.Provider;
import io.airlift.units.DataSize;
import io.trino.spi.connector.TableProcedureMetadata;

import static io.trino.plugin.archer.procedure.ArcherTableProcedureId.OPTIMIZE;
import static io.trino.plugin.base.session.PropertyMetadataUtil.dataSizeProperty;
import static io.trino.spi.connector.TableProcedureExecutionMode.distributedWithFilteringAndRepartitioning;
import static io.trino.spi.session.PropertyMetadata.booleanProperty;

public class OptimizeTableProcedure
        implements Provider<TableProcedureMetadata>
{
    public static final String FILE_SIZE_THRESHOLD = "file_size_threshold";
    public static final String REFRESH_PARTITION = "refresh_partition";
    public static final String REFRESH_INVERTED_INDEX = "refresh_inverted_index";

    @Override
    public TableProcedureMetadata get()
    {
        return new TableProcedureMetadata(
                OPTIMIZE.name(),
                distributedWithFilteringAndRepartitioning(),
                ImmutableList.of(
                        dataSizeProperty(
                                FILE_SIZE_THRESHOLD,
                                "Only compact files smaller than given threshold in bytes",
                                DataSize.of(100, DataSize.Unit.MEGABYTE),
                                false),
                        booleanProperty(
                                REFRESH_PARTITION,
                                "Rewrite this segment if partition spec is expired",
                                false,
                                false),
                        booleanProperty(
                                REFRESH_INVERTED_INDEX,
                                "Rewrite this segment if inverted index is expired",
                                false,
                                false)));
    }
}
