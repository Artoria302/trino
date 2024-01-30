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
package io.trino.plugin.archer.util;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

import static java.util.Objects.requireNonNull;

public class VersionedPath
{
    private final String path;
    private final int version;

    @JsonCreator
    public VersionedPath(@JsonProperty("path") String path, @JsonProperty("version") int version)
    {
        this.path = requireNonNull(path, "path is null");
        this.version = version;
    }

    @JsonProperty("path")
    public String getPath()
    {
        return path;
    }

    @JsonProperty("version")
    public int getVersion()
    {
        return version;
    }

    public net.qihoo.archer.VersionedPath toArcher()
    {
        return net.qihoo.archer.VersionedPath.of(path, version);
    }

    @Override
    public boolean equals(Object that)
    {
        if (this == that) {
            return true;
        }
        if (that == null || getClass() != that.getClass()) {
            return false;
        }
        VersionedPath other = (VersionedPath) that;
        return Objects.equals(this.path, other.path) && this.version == other.version;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(path, version);
    }
}
