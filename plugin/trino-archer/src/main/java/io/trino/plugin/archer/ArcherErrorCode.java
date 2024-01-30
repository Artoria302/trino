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

import io.trino.spi.ErrorCode;
import io.trino.spi.ErrorCodeSupplier;
import io.trino.spi.ErrorType;

import static io.trino.spi.ErrorType.EXTERNAL;
import static io.trino.spi.ErrorType.INTERNAL_ERROR;
import static io.trino.spi.ErrorType.USER_ERROR;

public enum ArcherErrorCode
        implements ErrorCodeSupplier
{
    // code ARCHER_UNKNOWN_TABLE_TYPE(0, EXTERNAL) is deprecated
    ARCHER_INVALID_METADATA(1, EXTERNAL),
    ARCHER_TOO_MANY_OPEN_PARTITIONS(2, USER_ERROR),
    ARCHER_INVALID_PARTITION_VALUE(3, EXTERNAL),
    ARCHER_BAD_DATA(4, EXTERNAL),
    // ARCHER_MISSING_DATA(5, EXTERNAL) is deprecated
    ARCHER_CANNOT_OPEN_SPLIT(6, EXTERNAL),
    ARCHER_WRITER_OPEN_ERROR(7, EXTERNAL),
    ARCHER_FILESYSTEM_ERROR(8, EXTERNAL),
    ARCHER_CURSOR_ERROR(9, EXTERNAL),
    ARCHER_WRITE_VALIDATION_FAILED(10, INTERNAL_ERROR),
    ARCHER_INVALID_SNAPSHOT_ID(11, USER_ERROR),
    ARCHER_COMMIT_ERROR(12, EXTERNAL),
    ARCHER_CATALOG_ERROR(13, EXTERNAL),
    ARCHER_WRITER_CLOSE_ERROR(14, EXTERNAL),
    ARCHER_MISSING_METADATA(15, EXTERNAL),
    ARCHER_WRITER_DATA_ERROR(16, EXTERNAL),
    ARCHER_READER_OPEN_ERROR(17, EXTERNAL),
    ARCHER_INVERTED_INDEX_QUERY_ERROR(18, USER_ERROR),
    ARCHER_UNSUPPORTED_INVERTED_INDEX_TYPE_ERROR(19, USER_ERROR),
    ARCHER_INTERNAL_ERROR(20, INTERNAL_ERROR),
    ARCHER_READ_LINE_ERROR(21, EXTERNAL),
    ARCHER_WRITE_LINE_ERROR(22, INTERNAL_ERROR),
    /**/;

    private final ErrorCode errorCode;

    ArcherErrorCode(int code, ErrorType type)
    {
        errorCode = new ErrorCode(code + 0x0360_0000, name(), type);
    }

    @Override
    public ErrorCode toErrorCode()
    {
        return errorCode;
    }
}
