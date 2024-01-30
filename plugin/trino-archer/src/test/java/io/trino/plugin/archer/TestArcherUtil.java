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

import org.junit.jupiter.api.Test;

import java.net.URI;

import static io.trino.hdfs.s3.TrinoS3FileSystem.extractBucketName;
import static io.trino.plugin.archer.ArcherUtil.parseVersion;
import static io.trino.plugin.archer.ArcherUtil.segmentFileName;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestArcherUtil
{
    @Test
    public void testParseVersion()
    {
        assertThat(parseVersion("00000-409702ba-4735-4645-8f14-09537cc0b2c8.metadata.json")).isEqualTo(0);
        assertThat(parseVersion("99999-409702ba-4735-4645-8f14-09537cc0b2c8.metadata.json")).isEqualTo(99999);
        assertThat(parseVersion("00010-409702ba-4735-4645-8f14-09537cc0b2c8.metadata.json")).isEqualTo(10);
        assertThat(parseVersion("00011-409702ba-4735-4645-8f14-09537cc0b2c8.metadata.json")).isEqualTo(11);
        assertThat(parseVersion("v0.metadata.json")).isEqualTo(0);
        assertThat(parseVersion("v10.metadata.json")).isEqualTo(10);
        assertThat(parseVersion("v99999.metadata.json")).isEqualTo(99999);
        assertThat(parseVersion("v0.gz.metadata.json")).isEqualTo(0);
        assertThat(parseVersion("v0.metadata.json.gz")).isEqualTo(0);

        assertThatThrownBy(() -> parseVersion("hdfs://hadoop-master:9000/user/hive/warehouse/orders_5-581fad8517934af6be1857a903559d44/metadata/00000-409702ba-4735-4645-8f14-09537cc0b2c8.metadata.json"))
                .hasMessageMatching("Not a file name: .*");
        assertThatThrownBy(() -> parseVersion("orders_5_581fad8517934af6be1857a903559d44"))
                .hasMessageMatching("Invalid metadata file name: .*");
        assertThatThrownBy(() -> parseVersion("metadata"))
                .hasMessageMatching("Invalid metadata file name:.*");
        assertThatThrownBy(() -> parseVersion("00010_409702ba_4735_4645_8f14_09537cc0b2c8.metadata.json"))
                .hasMessageMatching("Invalid metadata file name:.*");
        assertThatThrownBy(() -> parseVersion("v10_metadata_json"))
                .hasMessageMatching("Invalid metadata file name:.*");
        assertThatThrownBy(() -> parseVersion("v1..gz.metadata.json"))
                .hasMessageMatching("Invalid metadata file name:.*");
        assertThatThrownBy(() -> parseVersion("v1.metadata.json.gz."))
                .hasMessageMatching("Invalid metadata file name:.*");

        assertThatThrownBy(() -> parseVersion("00003_409702ba-4735-4645-8f14-09537cc0b2c8.metadata.json"))
                .hasMessageMatching("Invalid metadata file name:.*");
        assertThatThrownBy(() -> parseVersion("-00010-409702ba-4735-4645-8f14-09537cc0b2c8.metadata.json"))
                .hasMessageMatching("Invalid metadata file name:.*");
        assertThatThrownBy(() -> parseVersion("v-10.metadata.json"))
                .hasMessageMatching("Invalid metadata file name:.*");
    }

    @Test
    public void testSchema()
            throws Exception
    {
        assertThat(new URI("file:///").getScheme()).isEqualTo("file");
        assertThat(new URI("file:///path").getScheme()).isEqualTo("file");
        assertThat(new URI("file:///path/").getScheme()).isEqualTo("file");
        assertThat(new URI("file:///path/to").getScheme()).isEqualTo("file");
        assertThat(new URI("/").getScheme()).isNull();
        assertThat(new URI("/path").getScheme()).isNull();
        assertThat(new URI("/path/to").getScheme()).isNull();
        assertThat(new URI("s3://bucket/").getScheme()).isEqualTo("s3");
        assertThat(new URI("s3a://bucket/").getScheme()).isEqualTo("s3a");
        assertThat(new URI("s3n://bucket/").getScheme()).isEqualTo("s3n");

        {
            URI fullUri = new URI("s3a://bucket/abc");
            URI uri = new URI(fullUri.getScheme(), fullUri.getAuthority(), null, null, null);
            String bucketName = extractBucketName(uri);
            assertThat(bucketName).isEqualTo("bucket");
        }

        {
            URI fullUri = new URI("s3a://bucket/");
            URI uri = new URI(fullUri.getScheme(), fullUri.getAuthority(), null, null, null);
            String bucketName = extractBucketName(uri);
            assertThat(bucketName).isEqualTo("bucket");
        }

        {
            URI fullUri = new URI("s3a://bucket");
            URI uri = new URI(fullUri.getScheme(), fullUri.getAuthority(), null, null, null);
            String bucketName = extractBucketName(uri);
            assertThat(bucketName).isEqualTo("bucket");
        }
    }

    @Test
    public void testSegmentFiles()
    {
        assertThat(segmentFileName("hdfs://host/data/abc-abc/inverted_index/abc.test")).isEqualTo("abc-abc/inverted_index/abc.test");
        assertThat(segmentFileName("s3a://bucket/data/abc-abc/inverted_index/abc.test")).isEqualTo("abc-abc/inverted_index/abc.test");
        assertThat(segmentFileName("/host/data/abc-abc/inverted_index/abc.test")).isEqualTo("abc-abc/inverted_index/abc.test");
        assertThat(segmentFileName("s3a://bucket/data/abc-abc/abc.test")).isEqualTo("abc-abc/abc.test");
        assertThat(segmentFileName("/data/abc-abc/abc.test")).isEqualTo("abc-abc/abc.test");
        assertThatThrownBy(() -> segmentFileName("hdfs://host/data/abc-abc/inverted_index/abc/"));
        assertThatThrownBy(() -> segmentFileName("hdfs://host/data/abc-abc/inverted_index//abc.test"));
        assertThatThrownBy(() -> segmentFileName("/inverted_index/abc.test"));
        assertThatThrownBy(() -> segmentFileName("inverted_index/abc.test"));
    }
}
