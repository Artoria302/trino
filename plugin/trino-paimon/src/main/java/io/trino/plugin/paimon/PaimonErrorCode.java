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
package io.trino.plugin.paimon;

import io.trino.spi.ErrorCode;
import io.trino.spi.ErrorCodeSupplier;
import io.trino.spi.ErrorType;

import static io.trino.spi.ErrorType.EXTERNAL;
import static io.trino.spi.ErrorType.INTERNAL_ERROR;
import static io.trino.spi.ErrorType.USER_ERROR;

public enum PaimonErrorCode
        implements ErrorCodeSupplier
{
    PAIMON_INVALID_METADATA(1, EXTERNAL),
    PAIMON_INVALID_PARTITION_VALUE(2, EXTERNAL),
    PAIMON_BAD_DATA(3, EXTERNAL),
    PAIMON_CANNOT_OPEN_SPLIT(4, EXTERNAL),
    PAIMON_FILESYSTEM_ERROR(5, EXTERNAL),
    PAIMON_CURSOR_ERROR(6, EXTERNAL),
    PAIMON_COMMIT_ERROR(7, EXTERNAL),
    PAIMON_OPTIONS_ERROR(8, USER_ERROR),
    PAIMON_MISSING_METADATA(9, EXTERNAL),
    PAIMON_READER_OPEN_ERROR(10, EXTERNAL),
    PAIMON_INTERNAL_ERROR(11, INTERNAL_ERROR),
    PAIMON_READ_LINE_ERROR(12, EXTERNAL),
    /**/;

    private final ErrorCode errorCode;

    PaimonErrorCode(int code, ErrorType type)
    {
        errorCode = new ErrorCode(code + 0x0370_0000, name(), type);
    }

    @Override
    public ErrorCode toErrorCode()
    {
        return errorCode;
    }
}
