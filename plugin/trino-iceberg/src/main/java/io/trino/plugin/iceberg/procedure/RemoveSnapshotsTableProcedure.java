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
package io.trino.plugin.iceberg.procedure;

import com.google.common.collect.ImmutableList;
import com.google.inject.Provider;
import io.trino.spi.connector.TableProcedureMetadata;
import io.trino.spi.session.PropertyMetadata;
import io.trino.spi.type.ArrayType;

import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.iceberg.procedure.IcebergTableProcedureId.REMOVE_SNAPSHOTS;
import static io.trino.spi.connector.TableProcedureExecutionMode.coordinatorOnly;
import static io.trino.spi.session.PropertyMetadata.booleanProperty;
import static io.trino.spi.type.BigintType.BIGINT;

public class RemoveSnapshotsTableProcedure
        implements Provider<TableProcedureMetadata>
{
    public static final String SNAPSHOTS = "snapshots";
    public static final String CLEAN_FILES = "clean_files";

    @Override
    public TableProcedureMetadata get()
    {
        return new TableProcedureMetadata(
                REMOVE_SNAPSHOTS.name(),
                coordinatorOnly(),
                ImmutableList.of(
                        new PropertyMetadata<>(
                                SNAPSHOTS,
                                "Snapshots to remove, must be not null",
                                new ArrayType(BIGINT),
                                List.class,
                                ImmutableList.of(),
                                false,
                                value -> ((List<?>) value).stream()
                                        .map(Long.class::cast)
                                        .collect(toImmutableList()),
                                value -> value),
                        booleanProperty(
                                CLEAN_FILES,
                                "Clean files after remove snapshot",
                                true,
                                false)));
    }
}
