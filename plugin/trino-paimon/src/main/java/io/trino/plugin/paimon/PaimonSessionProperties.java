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

import com.google.inject.Inject;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.session.PropertyMetadata;
import org.apache.paimon.shade.guava30.com.google.common.collect.ImmutableList;

import java.util.List;

import static io.trino.spi.session.PropertyMetadata.booleanProperty;
import static io.trino.spi.session.PropertyMetadata.doubleProperty;
import static io.trino.spi.session.PropertyMetadata.integerProperty;
import static io.trino.spi.session.PropertyMetadata.longProperty;
import static org.apache.paimon.CoreOptions.SCAN_SNAPSHOT_ID;
import static org.apache.paimon.CoreOptions.SCAN_TIMESTAMP_MILLIS;

/**
 * Trino session properties.
 */
public class PaimonSessionProperties
{
    public static final String SCAN_TIMESTAMP = "scan_timestamp_millis";
    public static final String SCAN_SNAPSHOT = "scan_snapshot_id";
    public static final String MINIMUM_SPLIT_WEIGHT = "minimum_split_weight";
    private static final String LOCAL_CACHE_ENABLED = "local_cache_enabled";
    private static final String CACHE_NODE_COUNT = "cache_node_count";

    private final List<PropertyMetadata<?>> sessionProperties;

    @Inject
    public PaimonSessionProperties(PaimonConfig paimonConfig)
    {
        sessionProperties =
                ImmutableList.<PropertyMetadata<?>>builder()
                        .add(longProperty(
                                SCAN_TIMESTAMP,
                                SCAN_TIMESTAMP_MILLIS.description().toString(),
                                null,
                                true))
                        .add(longProperty(
                                SCAN_SNAPSHOT,
                                SCAN_SNAPSHOT_ID.description().toString(),
                                null,
                                true))
                        .add(doubleProperty(
                                MINIMUM_SPLIT_WEIGHT, "Minimum split weight", 0.05, false))
                        .add(booleanProperty(
                                LOCAL_CACHE_ENABLED,
                                "Is local cache enabled",
                                paimonConfig.isLocalCacheEnabled(),
                                false))
                        .add(integerProperty(
                                CACHE_NODE_COUNT,
                                "Cache node count",
                                paimonConfig.getCacheNodeCount(),
                                false))
                        .build();
    }

    public List<PropertyMetadata<?>> getSessionProperties()
    {
        return sessionProperties;
    }

    public static Long getScanTimestampMillis(ConnectorSession session)
    {
        return session.getProperty(SCAN_TIMESTAMP, Long.class);
    }

    public static Long getScanSnapshotId(ConnectorSession session)
    {
        return session.getProperty(SCAN_SNAPSHOT, Long.class);
    }

    public static Double getMinimumSplitWeight(ConnectorSession session)
    {
        return session.getProperty(MINIMUM_SPLIT_WEIGHT, Double.class);
    }

    public static boolean isLocalCacheEnabled(ConnectorSession session)
    {
        return session.getProperty(LOCAL_CACHE_ENABLED, Boolean.class);
    }

    public static int getCacheNodeCount(ConnectorSession session)
    {
        return session.getProperty(CACHE_NODE_COUNT, Integer.class);
    }
}
