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

import com.google.inject.Inject;
import net.qihoo.archer.arrow.RuntimeEnv;

import static com.google.common.base.Preconditions.checkState;

public class DefaultArcherRuntimeManager
        implements ArcherRuntimeManager
{
    private final ArcherRuntime defaultRuntime;

    @Inject
    public DefaultArcherRuntimeManager()
    {
        defaultRuntime = new ConstantRefCountArcherRuntime(RuntimeEnv.create(), 1);
    }

    @Override
    public ArcherRuntime get(String path)
            throws Exception
    {
        return defaultRuntime;
    }

    @Override
    public void free(ArcherRuntime runtime)
            throws Exception
    {
        long refCount;
        synchronized (this) {
            refCount = runtime.descAndGetRefCount();
        }
        checkState(refCount >= 0L, "refCount should always >= 0");
        if (refCount == 0) {
            runtime.close();
        }
    }
}
