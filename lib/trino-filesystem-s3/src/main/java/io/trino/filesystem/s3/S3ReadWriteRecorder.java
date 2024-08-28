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
package io.trino.filesystem.s3;

import io.airlift.log.Logger;
import software.amazon.awssdk.services.s3.model.S3ResponseMetadata;

import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;

public class S3ReadWriteRecorder
        implements AutoCloseable
{
    private static final Logger LOG = Logger.get(S3ReadWriteRecorder.class);

    private final String operation;
    private final String bucket;
    private final String key;
    private final Supplier<S3ResponseMetadata> metadataSupplier;
    private final long threshold;
    private final long startTimeMillis;

    S3ReadWriteRecorder(String operation, String bucket, String key, Supplier<S3ResponseMetadata> metadataSupplier, long thresholdMills)
    {
        this.operation = requireNonNull(operation, "label is null");
        this.bucket = requireNonNull(bucket, "bucket is null");
        this.key = requireNonNull(key, "key is null");
        this.metadataSupplier = requireNonNull(metadataSupplier, "response is null");
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
        LOG.warn("[%s] slow s3 request; bucket: %s, key: %s, took %d ms (threshold=%d ms); request metadata: %s",
                operation, bucket, key, latency, threshold, metadataSupplier.get());
    }

    public static class S3ResponseMetadataWrapper
    {
        private S3ResponseMetadata responseMetadata;

        public S3ResponseMetadata getResponseMetadata()
        {
            return responseMetadata;
        }

        public void setResponseMetadata(S3ResponseMetadata responseMetadata)
        {
            this.responseMetadata = responseMetadata;
        }
    }
}
