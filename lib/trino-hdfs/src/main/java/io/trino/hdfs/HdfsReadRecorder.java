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
package io.trino.hdfs;

import io.airlift.log.Logger;
import org.apache.hadoop.crypto.CryptoInputStream;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.hdfs.DFSInputStream;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;

import java.io.InputStream;

import static java.util.Objects.requireNonNull;

public class HdfsReadRecorder
        implements AutoCloseable
{
    private static final Logger log = Logger.get(HdfsReadRecorder.class);

    private final String context;
    private final FSDataInputStream delegate;
    private final String path;
    private final long threshold;
    private final long startTimeMillis;

    public HdfsReadRecorder(String context, String path, FSDataInputStream delegate, long thresholdMills)
    {
        this.context = requireNonNull(context, "info is null");
        this.path = requireNonNull(path, "path is null");
        this.delegate = requireNonNull(delegate, "delegate is null");
        this.threshold = thresholdMills;
        this.startTimeMillis = System.currentTimeMillis();
    }

    @Override
    public void close()
    {
        long latency = System.currentTimeMillis() - startTimeMillis;
        if (latency < threshold) {
            return;
        }

        DFSInputStream dfsInputStream = getDFSInputStream(delegate);
        if (dfsInputStream != null) {
            DatanodeInfo datanode = dfsInputStream.getCurrentDatanode();
            ExtendedBlock block = dfsInputStream.getCurrentBlock();
            log.warn("slow hdfs operate for block %s took %d ms (threshold=%d ms); current datanode: %s; path: %s; context: %s",
                    block, latency, threshold, datanode, path, context);
        }
        else {
            log.warn("slow hdfs operate took %d ms (threshold=%d ms); (unknown datanode); path: %s; context: %s",
                    latency, threshold, path, context);
        }
    }

    private static DFSInputStream getDFSInputStream(InputStream in)
    {
        while (true) {
            switch (in) {
                case DFSInputStream dfsInputStream -> {
                    return dfsInputStream;
                }
                case CryptoInputStream cryptoInputStream -> in = cryptoInputStream.getWrappedStream();
                case FSDataInputStream fsDataInputStream -> in = fsDataInputStream.getWrappedStream();
                case null, default -> {
                    return null;
                }
            }
        }
    }
}
