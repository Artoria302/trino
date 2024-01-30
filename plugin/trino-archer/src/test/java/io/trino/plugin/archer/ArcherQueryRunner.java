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
import com.google.common.collect.ImmutableMap;
import io.airlift.log.Level;
import io.airlift.log.Logger;
import io.airlift.log.Logging;
import io.trino.plugin.hive.containers.HiveMinioDataLake;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import io.trino.testing.containers.Minio;
import io.trino.tpch.TpchTable;

import java.io.File;
import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;
import static io.airlift.testing.Closeables.closeAllSuppress;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static io.trino.testing.containers.Minio.MINIO_ACCESS_KEY;
import static io.trino.testing.containers.Minio.MINIO_REGION;
import static io.trino.testing.containers.Minio.MINIO_SECRET_KEY;
import static java.nio.file.Files.createTempDirectory;
import static java.util.Objects.requireNonNull;

public final class ArcherQueryRunner
{
    private ArcherQueryRunner() {}

    public static final String ARCHER_CATALOG = "archer";

    static {
        Logging logging = Logging.initialize();
        logging.setLevel("org.apache.archer", Level.OFF);
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static Builder builder(String schema)
    {
        return new Builder(schema);
    }

    public static class Builder
            extends DistributedQueryRunner.Builder<Builder>
    {
        private Optional<File> metastoreDirectory = Optional.empty();
        private ImmutableMap.Builder<String, String> archerProperties = ImmutableMap.builder();
        private Optional<SchemaInitializer> schemaInitializer = Optional.empty();

        protected Builder()
        {
            super(testSessionBuilder()
                    .setCatalog(ARCHER_CATALOG)
                    .setSchema("tpch")
                    .build());
        }

        protected Builder(String schema)
        {
            super(testSessionBuilder()
                    .setCatalog(ARCHER_CATALOG)
                    .setSchema(schema)
                    .build());
        }

        public Builder setMetastoreDirectory(File metastoreDirectory)
        {
            this.metastoreDirectory = Optional.of(metastoreDirectory);
            return self();
        }

        public Builder setArcherProperties(Map<String, String> archerProperties)
        {
            this.archerProperties = ImmutableMap.<String, String>builder()
                    .putAll(requireNonNull(archerProperties, "archerProperties is null"));
            return self();
        }

        public Builder addArcherProperty(String key, String value)
        {
            this.archerProperties.put(key, value);
            return self();
        }

        public Builder setInitialTables(TpchTable<?>... initialTables)
        {
            return setInitialTables(ImmutableList.copyOf(initialTables));
        }

        public Builder setInitialTables(Iterable<TpchTable<?>> initialTables)
        {
            setSchemaInitializer(SchemaInitializer.builder().withClonedTpchTables(initialTables).build());
            return self();
        }

        public Builder setSchemaInitializer(SchemaInitializer schemaInitializer)
        {
            checkState(this.schemaInitializer.isEmpty(), "schemaInitializer is already set");
            this.schemaInitializer = Optional.of(requireNonNull(schemaInitializer, "schemaInitializer is null"));
            amendSession(sessionBuilder -> sessionBuilder.setSchema(schemaInitializer.getSchemaName()));
            return self();
        }

        @Override
        public DistributedQueryRunner build()
                throws Exception
        {
            DistributedQueryRunner queryRunner = super.build();
            try {
                queryRunner.installPlugin(new TpchPlugin());
                queryRunner.createCatalog("tpch", "tpch");

                Path dataDir = metastoreDirectory.map(File::toPath).orElseGet(() -> queryRunner.getCoordinator().getBaseDataDir().resolve("archer_data"));
                queryRunner.installPlugin(new TestingArcherPlugin(dataDir));
                queryRunner.createCatalog(ARCHER_CATALOG, "archer", archerProperties.buildOrThrow());
                schemaInitializer.orElseGet(() -> SchemaInitializer.builder().build()).accept(queryRunner);

                return queryRunner;
            }
            catch (Exception e) {
                closeAllSuppress(e, queryRunner);
                throw e;
            }
        }
    }

    public static final class ArcherMinioHiveMetastoreQueryRunnerMain
    {
        private ArcherMinioHiveMetastoreQueryRunnerMain() {}

        public static void main(String[] args)
                throws Exception
        {
            String bucketName = "test-bucket";
            @SuppressWarnings("resource")
            HiveMinioDataLake hiveMinioDataLake = new HiveMinioDataLake(bucketName);
            hiveMinioDataLake.start();

            @SuppressWarnings("resource")
            QueryRunner queryRunner = ArcherQueryRunner.builder()
                    .addCoordinatorProperty("http-server.http.port", "8080")
                    .setArcherProperties(Map.of(
                            "archer.catalog.type", "HIVE_METASTORE",
                            "hive.metastore.uri", hiveMinioDataLake.getHiveHadoop().getHiveMetastoreEndpoint().toString(),
                            "fs.hadoop.enabled", "false",
                            "fs.native-s3.enabled", "true",
                            "s3.aws-access-key", MINIO_ACCESS_KEY,
                            "s3.aws-secret-key", MINIO_SECRET_KEY,
                            "s3.region", MINIO_REGION,
                            "s3.endpoint", hiveMinioDataLake.getMinio().getMinioAddress(),
                            "s3.path-style-access", "true",
                            "s3.streaming.part-size", "5MB"))
                    .setSchemaInitializer(
                            SchemaInitializer.builder()
                                    .withSchemaName("tpch")
                                    .withClonedTpchTables(TpchTable.getTables())
                                    .withSchemaProperties(Map.of("location", "'s3://" + bucketName + "/tpch'"))
                                    .build())
                    .build();

            Logger log = Logger.get(ArcherMinioHiveMetastoreQueryRunnerMain.class);
            log.info("======== SERVER STARTED ========");
            log.info("\n====\n%s\n====", queryRunner.getCoordinator().getBaseUrl());
        }
    }

    public static final class ArcherMinioQueryRunnerMain
    {
        private ArcherMinioQueryRunnerMain() {}

        public static void main(String[] args)
                throws Exception
        {
            Logging.initialize();

            String bucketName = "test-bucket";
            @SuppressWarnings("resource")
            Minio minio = Minio.builder().build();
            minio.start();
            minio.createBucket(bucketName);

            @SuppressWarnings("resource")
            QueryRunner queryRunner = ArcherQueryRunner.builder()
                    .addCoordinatorProperty("http-server.http.port", "8080")
                    .setArcherProperties(Map.of(
                            "archer.catalog.type", "TESTING_FILE_METASTORE",
                            "hive.metastore.catalog.dir", "s3://%s/".formatted(bucketName),
                            "fs.hadoop.enabled", "false",
                            "fs.native-s3.enabled", "true",
                            "s3.aws-access-key", MINIO_ACCESS_KEY,
                            "s3.aws-secret-key", MINIO_SECRET_KEY,
                            "s3.region", MINIO_REGION,
                            "s3.endpoint", "http://" + minio.getMinioApiEndpoint(),
                            "s3.path-style-access", "true",
                            "s3.streaming.part-size", "5MB"))
                    .setSchemaInitializer(
                            SchemaInitializer.builder()
                                    .withSchemaName("tpch")
                                    .withClonedTpchTables(TpchTable.getTables())
                                    .build())
                    .build();

            Logger log = Logger.get(ArcherMinioQueryRunnerMain.class);
            log.info("======== SERVER STARTED ========");
            log.info("\n====\n%s\n====", queryRunner.getCoordinator().getBaseUrl());
        }
    }

    public static final class DefaultArcherQueryRunnerMain
    {
        private DefaultArcherQueryRunnerMain() {}

        public static void main(String[] args)
                throws Exception
        {
            Logger log = Logger.get(DefaultArcherQueryRunnerMain.class);
            File metastoreDir = createTempDirectory("archer_query_runner").toFile();
            metastoreDir.deleteOnExit();

            @SuppressWarnings("resource")
            QueryRunner queryRunner = ArcherQueryRunner.builder()
                    .addCoordinatorProperty("http-server.http.port", "8080")
                    .addArcherProperty("hive.metastore.catalog.dir", metastoreDir.toURI().toString())
                    .setInitialTables(TpchTable.getTables())
                    .build();
            log.info("======== SERVER STARTED ========");
            log.info("\n====\n%s\n====", queryRunner.getCoordinator().getBaseUrl());
        }
    }
}
