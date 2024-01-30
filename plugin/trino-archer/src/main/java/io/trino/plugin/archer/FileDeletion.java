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

import org.roaringbitmap.BitmapDataProvider;

import static java.util.Objects.requireNonNull;

public class FileDeletion
{
    private final int partitionSpecId;
    private final String partitionDataJson;
    private final int version;
    private final long fileRecordCount;
    private long deletedRecordCount;
    private final BitmapDataProvider rowsToDelete;

    public FileDeletion(int partitionSpecId, String partitionDataJson, int version, long fileRecordCount, long deletedRecordCount, BitmapDataProvider rowsToDelete)
    {
        this.partitionSpecId = partitionSpecId;
        this.partitionDataJson = requireNonNull(partitionDataJson, "partitionDataJson is null");
        this.version = version;
        this.fileRecordCount = fileRecordCount;
        this.deletedRecordCount = deletedRecordCount;
        this.rowsToDelete = requireNonNull(rowsToDelete, "rowsToDelete is null");
    }

    public int partitionSpecId()
    {
        return partitionSpecId;
    }

    public String partitionDataJson()
    {
        return partitionDataJson;
    }

    public long fileRecordCount()
    {
        return fileRecordCount;
    }

    public long deletedRecordCount()
    {
        return deletedRecordCount;
    }

    public BitmapDataProvider rowsToDelete()
    {
        return rowsToDelete;
    }

    public int version()
    {
        return version;
    }

    public void add(int pos)
    {
        if (!rowsToDelete.contains(pos)) {
            deletedRecordCount++;
            rowsToDelete.add(pos);
        }
    }
}
