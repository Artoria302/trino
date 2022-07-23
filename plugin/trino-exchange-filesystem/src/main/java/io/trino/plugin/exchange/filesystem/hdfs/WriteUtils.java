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

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FilterFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.viewfs.ViewFileSystem;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UncheckedIOException;

import static java.lang.String.format;

public final class WriteUtils
{
    private WriteUtils() {}

    public static boolean isViewFileSystem(ExchangeHdfsEnvironment hdfsEnvironment, Path path)
    {
        try {
            return getRawFileSystem(hdfsEnvironment.getFileSystem(path)) instanceof ViewFileSystem;
        }
        catch (IOException e) {
            throw new RuntimeException("Failed checking path: " + path, e);
        }
    }

    public static FileSystem getRawFileSystem(FileSystem fileSystem)
    {
        if (fileSystem instanceof FilterFileSystem) {
            return getRawFileSystem(((FilterFileSystem) fileSystem).getRawFileSystem());
        }
        return fileSystem;
    }

    private static void setDirectoryOwner(ExchangeHdfsEnvironment hdfsEnvironment, Path path, Path targetPath)
    {
        try {
            FileSystem fileSystem = hdfsEnvironment.getFileSystem(path);
            FileStatus fileStatus;
            if (!fileSystem.exists(targetPath)) {
                Path parent = targetPath.getParent();
                if (!fileSystem.exists(parent)) {
                    return;
                }
                fileStatus = fileSystem.getFileStatus(parent);
            }
            else {
                fileStatus = fileSystem.getFileStatus(targetPath);
            }
            String owner = fileStatus.getOwner();
            String group = fileStatus.getGroup();
            fileSystem.setOwner(path, owner, group);
        }
        catch (IOException e) {
            throw new RuntimeException(format("Failed to set owner on %s based on %s", path, targetPath), e);
        }
    }

    public static void createDirectory(ExchangeHdfsEnvironment hdfsEnvironment, Path path)
            throws IOException
    {
        if (!hdfsEnvironment.getFileSystem(path).mkdirs(path, hdfsEnvironment.getNewDirectoryPermissions().orElse(null))) {
            throw new IOException("mkdirs returned false, Failed to create directory: " + path);
        }

        if (hdfsEnvironment.getNewDirectoryPermissions().isPresent()) {
            // explicitly set permission since the default umask overrides it on creation
            hdfsEnvironment.getFileSystem(path).setPermission(path, hdfsEnvironment.getNewDirectoryPermissions().get());
        }
    }

    public static void createEmptyFile(ExchangeHdfsEnvironment hdfsEnvironment, Path file)
    {
        try {
            if (!hdfsEnvironment.getFileSystem(file).createNewFile(file)) {
                throw new IOException("createNewFile returned false");
            }
        }
        catch (IOException e) {
            throw new UncheckedIOException("Failed to create empty file: " + file, e);
        }
    }

    public static void checkedDelete(ExchangeHdfsEnvironment hdfsEnvironment, Path file, boolean recursive)
            throws IOException
    {
        FileSystem fileSystem = hdfsEnvironment.getFileSystem(file);
        try {
            if (!fileSystem.delete(file, recursive)) {
                if (fileSystem.exists(file)) {
                    // only throw exception if file still exists
                    throw new IOException("Failed to delete " + file);
                }
            }
        }
        catch (FileNotFoundException ignored) {
            // ok
        }
    }
}
