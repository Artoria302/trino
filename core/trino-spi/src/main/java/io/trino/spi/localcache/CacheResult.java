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

public class CacheResult
{
    private static final CacheResult MISS = new CacheResult(Result.MISS, -1);

    public Result getResult()
    {
        return result;
    }

    public int getLength()
    {
        return length;
    }

    public enum Result
    {
        HIT,
        MISS,
    }

    private final Result result;
    private final int length;

    private CacheResult(Result result, int length)
    {
        this.result = result;
        this.length = length;
    }

    public static CacheResult hit(int len)
    {
        return new CacheResult(Result.HIT, len);
    }

    public static CacheResult miss()
    {
        return MISS;
    }
}
