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
package io.trino.plugin.exchange.filesystem.hdfs.util;

import io.airlift.log.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.crypto.CipherSuite;
import org.apache.hadoop.crypto.CryptoCodec;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.crypto.CryptoFSDataInputStream;
import org.apache.hadoop.fs.crypto.CryptoFSDataOutputStream;
import org.apache.hadoop.io.IOUtils;

import javax.crypto.SecretKey;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Optional;

import static org.apache.hadoop.crypto.CryptoStreamUtils.getBufferSize;

public class CryptoUtils
{
    private static final Logger log = Logger.get(CryptoUtils.class);

    private CryptoUtils()
    {
    }

    public static byte[] createIV(Configuration conf)
            throws IOException
    {
        try (CryptoCodec cryptoCodec = CryptoCodec.getInstance(conf, CipherSuite.AES_CTR_NOPADDING)) {
            byte[] iv = new byte[cryptoCodec.getCipherSuite().getAlgorithmBlockSize()];
            cryptoCodec.generateSecureRandom(iv);
            return iv;
        }
    }

    public static int cryptoPadding(Optional<SecretKey> secretKey)
    {
        // Sizeof(IV) + long(start-offset)
        if (secretKey.isEmpty()) {
            return 0;
        }
        // try (CryptoCodec cryptoCodec = CryptoCodec.getInstance(conf, CipherSuite.AES_CTR_NOPADDING)) {
        //     return cryptoCodec.getCipherSuite().getAlgorithmBlockSize() + 8;
        // }
        return CipherSuite.AES_CTR_NOPADDING.getAlgorithmBlockSize() + 8;
    }

    public static FSDataOutputStream wrapIfNecessary(
            Configuration conf,
            Optional<SecretKey> secretKey,
            FSDataOutputStream out,
            boolean closeOutputStream)
            throws IOException
    {
        if (secretKey.isEmpty()) {
            return out;
        }
        out.write(ByteBuffer.allocate(8).putLong(out.getPos()).array());
        byte[] iv = createIV(conf);
        out.write(iv);
        if (log.isDebugEnabled()) {
            log.debug("IV written to Stream [%s]",
                    io.trino.hadoop.$internal.org.apache.commons.codec.binary.Base64.encodeBase64URLSafeString(iv));
        }
        return new CryptoFSDataOutputStream(out, CryptoCodec.getInstance(conf, CipherSuite.AES_CTR_NOPADDING), getBufferSize(conf), secretKey.get().getEncoded(), iv, closeOutputStream);
    }

    public static FSDataInputStream wrapIfNecessary(
            Configuration conf,
            Optional<SecretKey> secretKey,
            FSDataInputStream in)
            throws IOException
    {
        if (secretKey.isEmpty()) {
            return in;
        }
        CryptoCodec cryptoCodec = CryptoCodec.getInstance(conf, CipherSuite.AES_CTR_NOPADDING);
        // Not going to be used... but still has to be read...
        // Since the O/P stream always writes it..
        IOUtils.readFully(in, new byte[8], 0, 8);
        byte[] iv = new byte[cryptoCodec.getCipherSuite().getAlgorithmBlockSize()];
        IOUtils.readFully(in, iv, 0, cryptoCodec.getCipherSuite().getAlgorithmBlockSize());
        if (log.isDebugEnabled()) {
            log.debug("IV read from Stream [%s]",
                    io.trino.hadoop.$internal.org.apache.commons.codec.binary.Base64.encodeBase64URLSafeString(iv));
        }
        return new CryptoFSDataInputStream(in, cryptoCodec, getBufferSize(conf), secretKey.get().getEncoded(), iv);
    }
}
