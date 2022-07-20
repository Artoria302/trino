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
package io.trino.plugin.exchange.filesystem.hdfs;

import io.trino.hadoop.HadoopNative;
import io.trino.plugin.exchange.filesystem.hdfs.authentication.GenericExceptionAction;
import io.trino.plugin.exchange.filesystem.hdfs.authentication.HdfsAuthentication;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileSystemManager;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;

import javax.inject.Inject;

import java.io.IOException;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class ExchangeHdfsEnvironment
{
    static {
        HadoopNative.requireHadoopNative();
        FileSystemManager.registerCache(ExchangeFileSystemCache.INSTANCE);
    }

    private final Configuration hdfsConfiguration;
    private final HdfsAuthentication hdfsAuthentication;
    private final Optional<FsPermission> newDirectoryPermissions;
    private final boolean newFileInheritOwnership;
    private final boolean verifyChecksum;

    @Inject
    public ExchangeHdfsEnvironment(
            ExchangeHdfsConfig config,
            HdfsAuthentication hdfsAuthentication)
    {
        requireNonNull(config, "config is null");
        this.hdfsConfiguration = HdfsConfiguration.getConfiguration(config);
        this.newFileInheritOwnership = config.isNewFileInheritOwnership();
        this.verifyChecksum = config.isVerifyChecksum();
        this.hdfsAuthentication = requireNonNull(hdfsAuthentication, "hdfsAuthentication is null");
        this.newDirectoryPermissions = config.getNewDirectoryFsPermissions();
    }

    public FileSystem getFileSystem(Path path)
            throws IOException
    {
        return hdfsAuthentication.doAs(() -> {
            FileSystem fileSystem = path.getFileSystem(hdfsConfiguration);
            fileSystem.setVerifyChecksum(verifyChecksum);
            return fileSystem;
        });
    }

    public Optional<FsPermission> getNewDirectoryPermissions()
    {
        return newDirectoryPermissions;
    }

    public boolean isNewFileInheritOwnership()
    {
        return newFileInheritOwnership;
    }

    public <R, E extends Exception> R doAs(GenericExceptionAction<R, E> action)
            throws E
    {
        return hdfsAuthentication.doAs(action);
    }

    public void doAs(Runnable action)
    {
        hdfsAuthentication.doAs(action);
    }

    public Configuration getHdfsConfiguration()
    {
        return hdfsConfiguration;
    }
}
