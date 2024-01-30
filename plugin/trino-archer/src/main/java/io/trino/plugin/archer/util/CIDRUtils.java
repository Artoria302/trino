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

import java.math.BigInteger;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class CIDRUtils
{
    private final InetAddress startAddress;
    private final InetAddress endAddress;

    public CIDRUtils(String cidr)
    {
        /* split CIDR to address and prefix part */
        if (!cidr.contains("/")) {
            throw new IllegalArgumentException("Not an valid CIDR format");
        }

        int index = cidr.indexOf("/");
        String addressPart = cidr.substring(0, index);
        String networkPart = cidr.substring(index + 1);

        try {
            InetAddress inetAddress = InetAddress.getByName(addressPart);
            int prefixLength = Integer.parseInt(networkPart);

            ByteBuffer maskBuffer;
            int targetSize;
            if (inetAddress.getAddress().length == 4) {
                maskBuffer = ByteBuffer.allocate(4).putInt(-1);
                targetSize = 4;
            }
            else {
                maskBuffer = ByteBuffer.allocate(16).putLong(-1L).putLong(-1L);
                targetSize = 16;
            }

            BigInteger mask = (new BigInteger(1, maskBuffer.array())).not().shiftRight(prefixLength);

            ByteBuffer buffer = ByteBuffer.wrap(inetAddress.getAddress());
            BigInteger ipVal = new BigInteger(1, buffer.array());

            BigInteger startIp = ipVal.and(mask);
            BigInteger endIp = startIp.add(mask.not());

            byte[] startIpArr = toBytes(startIp.toByteArray(), targetSize);
            byte[] endIpArr = toBytes(endIp.toByteArray(), targetSize);

            this.startAddress = InetAddress.getByAddress(startIpArr);
            this.endAddress = InetAddress.getByAddress(endIpArr);
        }
        catch (Exception e) {
            throw new IllegalArgumentException("Not an valid CIDR format", e);
        }
    }

    private byte[] toBytes(byte[] array, int targetSize)
    {
        int counter = 0;
        List<Byte> newArr = new ArrayList<Byte>();
        while (counter < targetSize && (array.length - 1 - counter >= 0)) {
            newArr.add(0, array[array.length - 1 - counter]);
            counter++;
        }

        int size = newArr.size();
        for (int i = 0; i < (targetSize - size); i++) {
            newArr.add(0, (byte) 0);
        }

        byte[] ret = new byte[newArr.size()];
        for (int i = 0; i < newArr.size(); i++) {
            ret[i] = newArr.get(i);
        }
        return ret;
    }

    public String getNetworkAddress()
    {
        return this.startAddress.getHostAddress();
    }

    public String getBroadcastAddress()
    {
        return this.endAddress.getHostAddress();
    }

    public boolean inRange(String ipAddress)
    {
        try {
            InetAddress address = InetAddress.getByName(ipAddress);
            BigInteger start = new BigInteger(1, this.startAddress.getAddress());
            BigInteger end = new BigInteger(1, this.endAddress.getAddress());
            BigInteger target = new BigInteger(1, address.getAddress());

            int st = start.compareTo(target);
            int te = target.compareTo(end);

            return (st == -1 || st == 0) && (te == -1 || te == 0);
        }
        catch (UnknownHostException e) {
            throw new IllegalArgumentException(e);
        }
    }
}
