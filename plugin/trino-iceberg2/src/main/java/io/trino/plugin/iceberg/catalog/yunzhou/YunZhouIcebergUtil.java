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
package io.trino.plugin.iceberg.catalog.yunzhou;

import com.google.common.base.Joiner;
import com.google.common.collect.Iterables;
import com.google.common.collect.MapMaker;
import io.airlift.log.Logger;
import io.trino.metastore.HiveMetastore;
import io.trino.plugin.iceberg.IcebergUtil;
import org.apache.iceberg.ManifestEntry;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.ManifestReader;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.iceberg.util.Tasks;
import org.apache.iceberg.util.ThreadPools;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static java.lang.String.format;
import static org.apache.iceberg.TableProperties.GC_ENABLED;
import static org.apache.iceberg.TableProperties.GC_ENABLED_DEFAULT;

public final class YunZhouIcebergUtil
{
    private YunZhouIcebergUtil() {}

    private static final Logger log = Logger.get(IcebergUtil.class);

    /**
     * Drops all data and metadata files referenced by TableMetadata.
     * <p>
     * This should be called by dropTable implementations to clean up table files once the table has been dropped in the
     * metastore.
     *
     * @param metastore a metastore to use for deletes
     * @param metadata the dropped table.
     */
    public static void dropTableData(HiveMetastore metastore, FileIO io, TableMetadata metadata)
    {
        // Reads and deletes are done using Tasks.foreach(...).suppressFailureWhenFinished to complete
        // as much of the delete work as possible and avoid orphaned data or manifest files.

        Set<String> manifestListsToDelete = new HashSet<>();
        Set<ManifestFile> manifestsToDelete = new HashSet<>();
        for (Snapshot snapshot : metadata.snapshots()) {
            // add all manifests to the delete set because both data and delete files should be removed
            Iterables.addAll(manifestsToDelete, snapshot.allManifests(io));
            // add the manifest list to the delete set, if present
            if (snapshot.manifestListLocation() != null) {
                manifestListsToDelete.add(snapshot.manifestListLocation());
            }
        }

        log.info("Manifests to delete: %s", Joiner.on(", ").join(manifestsToDelete));

        // run all of the deletes

        boolean gcEnabled = PropertyUtil.propertyAsBoolean(metadata.properties(), GC_ENABLED, GC_ENABLED_DEFAULT);

        if (gcEnabled) {
            // delete data files only if we are sure this won't corrupt other tables
            deleteFiles(io, manifestsToDelete);
        }

        Tasks.foreach(Iterables.transform(manifestsToDelete, ManifestFile::path))
                .executeWith(ThreadPools.getWorkerPool())
                .noRetry().suppressFailureWhenFinished()
                .onFailure((manifest, exc) -> log.warn("Delete failed for manifest: %s, caused by %s", manifest, exc.getCause()))
                .run(io::deleteFile);

        Tasks.foreach(manifestListsToDelete)
                .executeWith(ThreadPools.getWorkerPool())
                .noRetry().suppressFailureWhenFinished()
                .onFailure((list, exc) -> log.warn("Delete failed for manifest list: %s, caused by %s", list, exc.getCause()))
                .run(io::deleteFile);

        dropYunZhouMetadata(metastore, metadata);
    }

    public static void dropYunZhouMetadata(HiveMetastore metastore, TableMetadata metadata)
    {
        Tasks.foreach(Iterables.transform(metadata.previousFiles(), TableMetadata.MetadataLogEntry::metadataId))
                .executeWith(ThreadPools.getWorkerPool())
                .noRetry().suppressFailureWhenFinished()
                .onFailure((metadataFile, exc) -> log.warn("Delete failed for previous metadata file: {}", metadataFile, exc))
                .run(metadataId -> deleteMetadataId(metastore, metadataId));

        Tasks.foreach(metadata.metadataId())
                .noRetry().suppressFailureWhenFinished()
                .onFailure((list, exc) -> log.warn("Delete failed for metadata Id: %s, caused by %s", list, exc.getCause()))
                .run(metadataId -> deleteMetadataId(metastore, metadataId));
    }

    private static void deleteMetadataId(HiveMetastore metastore, String metadataId)
    {
        try {
            boolean deleted = metastore.deleteMetadata(metadataId);
            long deleteCounts = metastore.deleteAllSnapshots(metadataId);
            log.info("Deleted %s snapshots for metadata %s", deleteCounts, metadataId);
            if (!deleted) {
                log.error("Fail to cleanup metadata ID at %s", metadataId);
                throw new RuntimeException("Fail to cleanup metadata ID at " + metadataId);
            }
        }
        catch (Exception e) {
            log.error("Fail to cleanup metadata ID at %s: ", metadataId, e.getCause());
            throw new RuntimeException(e.getMessage());
        }
    }

    private static void deleteFiles(FileIO io, Set<ManifestFile> allManifests)
    {
        // keep track of deleted files in a map that can be cleaned up when memory runs low
        Map<String, Boolean> deletedFiles = new MapMaker()
                .concurrencyLevel(ThreadPools.WORKER_THREAD_POOL_SIZE)
                .weakKeys()
                .makeMap();

        Tasks.foreach(allManifests)
                .noRetry().suppressFailureWhenFinished()
                .executeWith(ThreadPools.getWorkerPool())
                .onFailure((item, exc) -> log.warn("Failed to get deleted files: this may cause orphaned data files:%s", exc.getCause()))
                .run(manifest -> {
                    try (ManifestReader<?> reader = ManifestFiles.open(manifest, io)) {
                        for (ManifestEntry<?> entry : reader.entries()) {
                            // intern the file path because the weak key map uses identity (==) instead of equals
                            String path = entry.file().path().toString().intern();
                            Boolean alreadyDeleted = deletedFiles.putIfAbsent(path, true);
                            if (alreadyDeleted == null || !alreadyDeleted) {
                                try {
                                    io.deleteFile(path);
                                }
                                catch (RuntimeException e) {
                                    // this may happen if the map of deleted files gets cleaned up by gc
                                    log.warn("Delete failed for data file: %s, caused by ", path, e.getCause());
                                }
                            }
                        }
                    }
                    catch (IOException e) {
                        throw new UncheckedIOException(format("Failed to read manifest file: %s", manifest.path()), e);
                    }
                });
    }
}
