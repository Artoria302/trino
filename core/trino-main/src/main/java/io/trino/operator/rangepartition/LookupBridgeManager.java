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
package io.trino.operator.rangepartition;

import java.util.concurrent.atomic.AtomicBoolean;

import static java.util.Objects.requireNonNull;

public class LookupBridgeManager<T extends LookupBridge>
{
    private final AtomicBoolean initialized = new AtomicBoolean();
    private final T lookupBridge;

    public LookupBridgeManager(
            T joinBridge)
    {
        this.lookupBridge = requireNonNull(joinBridge, "joinBridge is null");
    }

    private void initializeIfNecessary()
    {
        if (!initialized.get()) {
            synchronized (this) {
                if (initialized.get()) {
                    return;
                }
                initialized.set(true);
            }
        }
    }

    public T getLookupBridge()
    {
        initializeIfNecessary();
        return lookupBridge;
    }
}
