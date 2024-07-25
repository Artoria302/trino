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

import com.google.errorprone.annotations.ThreadSafe;
import io.airlift.slice.Slice;

@ThreadSafe
public interface CacheManager
{
    CacheResult readFully(FileReadRequest request, byte[] buffer, int offset);

    CacheResult read(FileReadRequest request, byte[] buffer, int offset);

    void write(FileReadRequest request, Slice data);

    void destroy();

    default boolean isValid()
    {
        return false;
    }
}
