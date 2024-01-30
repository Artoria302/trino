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
package io.trino.plugin.archer.runtime;

import net.qihoo.archer.arrow.RuntimeEnv;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class ConstantRefCountArcherRuntime
        extends ArcherRuntime
{
    private final RuntimeEnv runtimeEnv;
    private final long refCount;

    public ConstantRefCountArcherRuntime(RuntimeEnv runtimeEnv, long initialRefCount)
    {
        checkArgument(initialRefCount >= 0, "initialRefCount should >= 0");
        this.refCount = initialRefCount;
        this.runtimeEnv = requireNonNull(runtimeEnv, "runtimeEnv is null");
    }

    @Override
    public long getRefCount()
    {
        return refCount;
    }

    @Override
    public long incAndGetRefCount()
    {
        return refCount;
    }

    @Override
    public long descAndGetRefCount()
    {
        return refCount;
    }

    @Override
    public RuntimeEnv getRuntimeEnv()
    {
        return runtimeEnv;
    }

    @Override
    void close()
            throws Exception
    {
        runtimeEnv.close();
    }
}
