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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.trino.spi.Plugin;
import io.trino.spi.connector.ConnectorFactory;

import java.util.Set;

import static io.trino.plugin.archer.function.InvertedIndexMatchFunction.INV_MATCH_FUNCTIONS;
import static io.trino.plugin.archer.function.InvertedIndexRangeFunction.INV_MATCH_RANGE_FUNCTIONS;
import static io.trino.plugin.archer.function.PartitionFunction.PARTITION_FUNCTIONS;

public class ArcherPlugin
        implements Plugin
{
    @Override
    public Iterable<ConnectorFactory> getConnectorFactories()
    {
        return ImmutableList.of(new ArcherConnectorFactory());
    }

    @Override
    public Set<Class<?>> getFunctions()
    {
        return ImmutableSet.<Class<?>>builder()
                .addAll(INV_MATCH_FUNCTIONS)
                .addAll(INV_MATCH_RANGE_FUNCTIONS)
                .addAll(PARTITION_FUNCTIONS)
                .build();
    }
}
