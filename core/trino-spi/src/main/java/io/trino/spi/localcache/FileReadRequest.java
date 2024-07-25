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
package io.trino.spi.localcache;

import java.util.Objects;

import static java.util.Objects.requireNonNull;

public class FileReadRequest
{
    private final FileIdentifier fileIdentifier;
    private final long offset;
    private final int length;

    public FileReadRequest(String path, long modificationTime, long offset, int length)
    {
        this.fileIdentifier = new FileIdentifier(requireNonNull(path, "path is null"), modificationTime);
        this.offset = offset;
        this.length = length;
    }

    public FileReadRequest(FileIdentifier fileIdentifier, long offset, int length)
    {
        this.fileIdentifier = requireNonNull(fileIdentifier, "fileIdentifier is null");
        this.offset = offset;
        this.length = length;
    }

    public FileIdentifier getFileIdentifier()
    {
        return fileIdentifier;
    }

    public long getOffset()
    {
        return offset;
    }

    public int getLength()
    {
        return length;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(fileIdentifier, offset, length);
    }

    @Override
    public boolean equals(Object object)
    {
        if (this == object) {
            return true;
        }
        if (!(object instanceof FileReadRequest other)) {
            return false;
        }
        return Objects.equals(this.fileIdentifier, other.fileIdentifier) &&
                this.offset == other.offset &&
                this.length == other.length;
    }
}
