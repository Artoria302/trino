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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.connector.ConnectorTableExecuteHandle;
import io.trino.spi.connector.SchemaTableName;

import static java.util.Objects.requireNonNull;

public record ArcherTableExecuteHandle(SchemaTableName schemaTableName, ArcherTableProcedureId procedureId, ArcherProcedureHandle procedureHandle, String tableLocation)
        implements ConnectorTableExecuteHandle
{
    @JsonCreator
    public ArcherTableExecuteHandle(
            SchemaTableName schemaTableName,
            ArcherTableProcedureId procedureId,
            ArcherProcedureHandle procedureHandle,
            String tableLocation)
    {
        this.schemaTableName = requireNonNull(schemaTableName, "schemaTableName is null");
        this.procedureId = requireNonNull(procedureId, "procedureId is null");
        this.procedureHandle = requireNonNull(procedureHandle, "procedureHandle is null");
        this.tableLocation = requireNonNull(tableLocation, "tableLocation is null");
    }

    @Override
    @JsonProperty
    public SchemaTableName schemaTableName()
    {
        return schemaTableName;
    }

    @Override
    @JsonProperty
    public ArcherTableProcedureId procedureId()
    {
        return procedureId;
    }

    @Override
    @JsonProperty
    public ArcherProcedureHandle procedureHandle()
    {
        return procedureHandle;
    }

    @Override
    @JsonProperty
    public String tableLocation()
    {
        return tableLocation;
    }

    public ArcherTableExecuteHandle withProcedureHandle(ArcherProcedureHandle procedureHandle)
    {
        return new ArcherTableExecuteHandle(
                schemaTableName,
                procedureId,
                procedureHandle,
                tableLocation);
    }

    @Override
    public String toString()
    {
        return new StringBuilder()
                .append("schemaTableName").append(":").append(schemaTableName)
                .append(", procedureId").append(":").append(procedureId)
                .append(", procedureHandle").append(":{").append(procedureHandle).append("}")
                .toString();
    }
}
