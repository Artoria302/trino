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
package io.trino.localcache.filemerge;

import java.util.concurrent.Semaphore;

class TestingCacheStats
        extends FileMergeCacheStats
{
    private final Semaphore semaphore;
    private final int permits;

    public TestingCacheStats()
    {
        this.semaphore = new Semaphore(1);
        this.permits = 1;
    }

    public TestingCacheStats(int permits)
    {
        this.semaphore = new Semaphore(permits);
        this.permits = permits;
    }

    @Override
    public void addInMemoryRetainedBytes(long bytes)
    {
        super.addInMemoryRetainedBytes(bytes);
        if (bytes > 0) {
            try {
                semaphore.acquire();
            }
            catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        if (bytes < 0) {
            semaphore.release();
        }
    }

    public void trigger()
    {
        try {
            semaphore.acquire(permits);
        }
        catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        semaphore.release(permits);
    }
}
