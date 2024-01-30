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

import io.trino.plugin.archer.util.CIDRUtils;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class TestSubnetUtils
{
    @Test
    public void testAddresses()
    {
        CIDRUtils utils = new CIDRUtils("192.168.0.1/29");
        assertThat(utils.inRange("192.168.0.1")).isTrue();
        // count the broadcast address as usable
        assertThat(utils.inRange("192.168.0.7")).isTrue();
        assertThat(utils.inRange("192.168.0.8")).isFalse();
        assertThat(utils.inRange("10.10.2.1")).isFalse();
        assertThat(utils.inRange("192.168.1.1")).isFalse();
        assertThat(utils.inRange("192.168.0.255")).isFalse();
    }

    /*
     * Address range test of IPv6 address
     *
     * Relate to NET-405
     */
    @Test
    public void testIsInRangeOfIP6Address()
    {
        CIDRUtils utils = new CIDRUtils("2001:db8:3c0d:5b6d:0:0:42:8329/58");
        assertThat(utils.inRange("2001:db8:3c0d:5b6d:0:0:42:8329")).isTrue();
        assertThat(utils.inRange("2001:db8:3c0d:5b40::")).isTrue();
        assertThat(utils.inRange("2001:db8:3c0d:5b7f:ffff:ffff:ffff:ffff")).isTrue();
        assertThat(utils.inRange("2001:db8:3c0d:5b53:0:0:0:1")).isTrue();
        assertThat(utils.inRange("2001:db8:3c0d:5b3f:ffff:ffff:ffff:ffff")).isFalse();
        assertThat(utils.inRange("2001:db8:3c0d:5b80::")).isFalse();
    }

    @Test
    public void testBoundaryIsInRangeOfIP6Address()
    {
        CIDRUtils utils = new CIDRUtils("2001:db8:3c0d:5b6d:0:0:42:8329/128");
        assertThat(utils.inRange("2001:db8:3c0d:5b6d:0:0:42:8329")).isTrue();
        assertThat(utils.inRange("2001:db8:3c0d:5b6d:0:0:42:8329")).isTrue();
    }
}
