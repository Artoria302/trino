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

import io.trino.spi.TrinoException;
import net.qihoo.archer.FileFormat;

import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;

public enum ArcherFileFormat
{
    PARQUET,
    /**/;

    public FileFormat toArcher()
    {
        return switch (this) {
            case PARQUET -> FileFormat.PARQUET;
        };
    }

    public static ArcherFileFormat fromArcher(FileFormat format)
    {
        return switch (format) {
            case PARQUET -> PARQUET;
            default -> throw new TrinoException(NOT_SUPPORTED, "File format not supported for Archer: " + format);
        };
    }
}
