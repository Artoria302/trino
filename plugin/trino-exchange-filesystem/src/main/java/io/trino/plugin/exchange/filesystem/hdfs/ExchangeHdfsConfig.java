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

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Shorts;
import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.validation.FileExists;
import io.airlift.units.DataSize;
import io.airlift.units.MaxDataSize;
import io.airlift.units.MinDataSize;
import org.apache.hadoop.fs.permission.FsPermission;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Pattern;

import java.io.File;
import java.util.List;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static java.lang.Integer.parseUnsignedInt;
import static java.util.Objects.requireNonNull;

public class ExchangeHdfsConfig
{
    public static final String SKIP_DIR_PERMISSIONS = "skip";
    private static final Splitter SPLITTER = Splitter.on(',').trimResults().omitEmptyStrings();

    private HdfsAuthenticationType hdfsAuthenticationType = HdfsAuthenticationType.NONE;
    private String hdfsPrincipal;
    private String hdfsKeytab;

    private List<File> resourceConfigFiles = ImmutableList.of();
    private String newDirectoryPermissions = "0777";
    private boolean newFileInheritOwnership;
    private boolean verifyChecksum = true;

    private DataSize hdfsStorageBlockSize = DataSize.of(4, MEGABYTE);
    private int maxBackgroundThreads = 1_000;

    public enum HdfsAuthenticationType
    {
        NONE,
        KERBEROS,
    }

    @NotNull
    public HdfsAuthenticationType getHdfsAuthenticationType()
    {
        return hdfsAuthenticationType;
    }

    @Config("exchange.hdfs.authentication.type")
    @ConfigDescription("HDFS authentication type")
    public ExchangeHdfsConfig setHdfsAuthenticationType(HdfsAuthenticationType hdfsAuthenticationType)
    {
        this.hdfsAuthenticationType = hdfsAuthenticationType;
        return this;
    }

    @NotNull
    public String getHdfsPrincipal()
    {
        return hdfsPrincipal;
    }

    @Config("exchange.hdfs.krb5.principal")
    @ConfigDescription("HDFS kerberos principal")
    public ExchangeHdfsConfig setHdfsPrincipal(String principal)
    {
        this.hdfsPrincipal = principal;
        return this;
    }

    @NotNull
    public String getHdfsKeytab()
    {
        return hdfsKeytab;
    }

    @Config("exchange.hdfs.krb5.keytab")
    @ConfigDescription("HDFS kerberos keytab")
    public ExchangeHdfsConfig setHdfsKeytab(String keytab)
    {
        this.hdfsKeytab = keytab;
        return this;
    }

    @NotNull
    @MinDataSize("4MB")
    @MaxDataSize("256MB")
    public DataSize getHdfsStorageBlockSize()
    {
        return hdfsStorageBlockSize;
    }

    @Config("exchange.hdfs.block-size")
    @ConfigDescription("Block size for Hdfs High-Throughput Block Size")
    public ExchangeHdfsConfig setAzureStorageBlockSize(DataSize hdfsStorageBlockSize)
    {
        this.hdfsStorageBlockSize = hdfsStorageBlockSize;
        return this;
    }

    @NotNull
    public List<@FileExists File> getResourceConfigFiles()
    {
        return resourceConfigFiles;
    }

    @Config("exchange.hdfs.config.resources")
    public ExchangeHdfsConfig setResourceConfigFiles(String files)
    {
        this.resourceConfigFiles = SPLITTER.splitToList(files).stream()
                .map(File::new)
                .collect(toImmutableList());
        return this;
    }

    public ExchangeHdfsConfig setResourceConfigFiles(List<File> files)
    {
        this.resourceConfigFiles = ImmutableList.copyOf(files);
        return this;
    }

    public Optional<FsPermission> getNewDirectoryFsPermissions()
    {
        if (newDirectoryPermissions.equalsIgnoreCase(SKIP_DIR_PERMISSIONS)) {
            return Optional.empty();
        }
        return Optional.of(FsPermission.createImmutable(Shorts.checkedCast(parseUnsignedInt(newDirectoryPermissions, 8))));
    }

    @Pattern(regexp = "(skip)|0[0-7]{3}", message = "must be either 'skip' or an octal number, with leading 0")
    public String getNewDirectoryPermissions()
    {
        return this.newDirectoryPermissions;
    }

    @Config("exchange.hdfs.new-directory-permissions")
    @ConfigDescription("File system permissions for new directories")
    public ExchangeHdfsConfig setNewDirectoryPermissions(String newDirectoryPermissions)
    {
        this.newDirectoryPermissions = requireNonNull(newDirectoryPermissions, "newDirectoryPermissions is null");
        return this;
    }

    public boolean isNewFileInheritOwnership()
    {
        return newFileInheritOwnership;
    }

    @Config("exchange.hdfs.new-file-inherit-ownership")
    @ConfigDescription("File system permissions for new directories")
    public ExchangeHdfsConfig setNewFileInheritOwnership(boolean newFileInheritOwnership)
    {
        this.newFileInheritOwnership = newFileInheritOwnership;
        return this;
    }

    public boolean isVerifyChecksum()
    {
        return verifyChecksum;
    }

    @Config("exchange.hdfs.verify-checksum")
    public ExchangeHdfsConfig setVerifyChecksum(boolean verifyChecksum)
    {
        this.verifyChecksum = verifyChecksum;
        return this;
    }

    @Min(1)
    public int getMaxBackgroundThreads()
    {
        return maxBackgroundThreads;
    }

    @Config("exchange.hdfs.max-background-threads")
    public ExchangeHdfsConfig setMaxBackgroundThreads(int maxBackgroundThreads)
    {
        this.maxBackgroundThreads = maxBackgroundThreads;
        return this;
    }
}
