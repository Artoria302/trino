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
package io.trino.plugin.paimon;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.filesystem.cache.CachingHostAddressProvider;
import io.trino.spi.HostAddress;
import io.trino.spi.SplitWeight;
import io.trino.spi.connector.ConnectorSplit;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.Split;

import java.util.List;

import static java.util.Collections.emptyList;
import static java.util.Objects.requireNonNull;

/**
 * Trino {@link ConnectorSplit}.
 */
public class PaimonSplit
        implements ConnectorSplit
{
    private final String splitSerialized;
    private final Double weight;
    private final List<HostAddress> addresses;

    @JsonCreator
    public PaimonSplit(
            @JsonProperty("splitSerialized") String splitSerialized,
            @JsonProperty("weight") Double weight)
    {
        this(splitSerialized, weight, emptyList());
    }

    public PaimonSplit(
            String splitSerialized,
            Double weight,
            List<HostAddress> addresses)
    {
        this.splitSerialized = splitSerialized;
        this.weight = weight;
        this.addresses = requireNonNull(addresses, "addresses is null");
    }

    public static PaimonSplit fromSplit(Split split, Double weight, CachingHostAddressProvider cachingHostAddressProvider, boolean localCacheEnabled, int cacheNodeCount)
    {
        List<HostAddress> addresses = emptyList();
        if (localCacheEnabled && split instanceof DataSplit dataSplit && dataSplit.bucketPath() != null && !dataSplit.bucketPath().isEmpty()) {
            addresses = cachingHostAddressProvider.getHosts(dataSplit.bucketPath(), cacheNodeCount, addresses);
        }
        return new PaimonSplit(EncodingUtils.encodeObjectToString(split), weight, addresses);
    }

    public Split decodeSplit()
    {
        return EncodingUtils.decodeStringToObject(splitSerialized);
    }

    @JsonProperty
    public String getSplitSerialized()
    {
        return splitSerialized;
    }

    @JsonProperty
    public Double getWeight()
    {
        return weight;
    }

    @JsonIgnore
    @Override
    public List<HostAddress> getAddresses()
    {
        return addresses;
    }

    @Override
    public SplitWeight getSplitWeight()
    {
        return SplitWeight.fromProportion(weight);
    }
}
