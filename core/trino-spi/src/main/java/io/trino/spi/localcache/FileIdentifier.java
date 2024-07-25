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

public class FileIdentifier
{
    private final String path;
    private final long modificationTime;

    public FileIdentifier(String path, long modificationTime)
    {
        this.path = path;
        this.modificationTime = modificationTime;
    }

    public String getPath()
    {
        return path;
    }

    public long getModificationTime()
    {
        return modificationTime;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(path, modificationTime);
    }

    @Override
    public boolean equals(Object object)
    {
        if (this == object) {
            return true;
        }
        if (!(object instanceof FileIdentifier other)) {
            return false;
        }
        return Objects.equals(this.path, other.path) &&
                this.modificationTime == other.modificationTime;
    }
}
