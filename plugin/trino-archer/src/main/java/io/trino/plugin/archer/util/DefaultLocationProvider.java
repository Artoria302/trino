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

import net.qihoo.archer.PartitionSpec;
import net.qihoo.archer.StructLike;
import net.qihoo.archer.TableProperties;
import net.qihoo.archer.io.LocationProvider;

import java.util.Map;

import static java.lang.String.format;
import static net.qihoo.archer.util.LocationUtil.stripTrailingSlash;

// based on net.qihoo.archer.LocationProviders.DefaultLocationProvider
public class DefaultLocationProvider
        implements LocationProvider
{
    private final String dataLocation;

    public DefaultLocationProvider(String tableLocation, Map<String, String> properties)
    {
        this.dataLocation = stripTrailingSlash(dataLocation(properties, tableLocation));
    }

    @SuppressWarnings("deprecation")
    private static String dataLocation(Map<String, String> properties, String tableLocation)
    {
        String dataLocation = properties.get(TableProperties.WRITE_DATA_LOCATION);
        if (dataLocation == null) {
            dataLocation = format("%s/data", stripTrailingSlash(tableLocation));
        }
        return dataLocation;
    }

    @Override
    public String newDataLocation(PartitionSpec spec, StructLike partitionData, String filename)
    {
        return "%s/%s/%s".formatted(dataLocation, spec.partitionToPath(partitionData), filename);
    }

    @Override
    public String newDataLocation(String filename)
    {
        return "%s/%s".formatted(dataLocation, filename);
    }
}
