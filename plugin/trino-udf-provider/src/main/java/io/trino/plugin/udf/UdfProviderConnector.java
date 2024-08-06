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
package io.trino.plugin.udf;

import com.google.common.collect.ImmutableSet;
import io.airlift.log.Logger;
import io.trino.filesystem.FileIterator;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.spi.connector.Connector;
import io.trino.spi.function.AggregationFunction;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.ScalarOperator;
import io.trino.spi.security.ConnectorIdentity;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.DirectoryStream;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

import static com.google.common.base.Verify.verify;
import static com.google.common.collect.Streams.stream;
import static java.nio.file.Files.newDirectoryStream;
import static java.util.Collections.emptySet;
import static java.util.UUID.randomUUID;

public class UdfProviderConnector
        implements Connector
{
    private static final Logger log = Logger.get(UdfProviderConnector.class);

    private final Set<Class<?>> functions;

    public UdfProviderConnector(UdfProviderConfig udfProviderConfig, TrinoFileSystemFactory trinoFileSystemFactory)
    {
        if (udfProviderConfig.getLocation().isEmpty()) {
            this.functions = emptySet();
        }
        else {
            Optional<File> directory = downloadJars(trinoFileSystemFactory, udfProviderConfig.getUser(), udfProviderConfig.getLocation().get());
            this.functions = directory.map(dir -> createFunctions(dir, udfProviderConfig.getPackage())).orElse(emptySet());
        }
    }

    @Override
    public Set<Class<?>> getFunctions()
    {
        return functions;
    }

    private static Set<Class<?>> createFunctions(File udfDirectory, Optional<String> packageName)
    {
        List<URL> urls = buildClassPath(udfDirectory);
        ClassLoader classLoader = new UdfProviderClassLoader(urls, UdfProviderConnector.class.getClassLoader());
        return loadFunctionClass(urls, classLoader, packageName);
    }

    private static Set<Class<?>> loadFunctionClass(List<URL> urls, ClassLoader classLoader, Optional<String> packageName)
    {
        ImmutableSet.Builder<Class<?>> functions = ImmutableSet.builder();
        try {
            for (URL url : urls) {
                URI uri = url.toURI();
                if ("file".equals(uri.getScheme())) {
                    File file = new File(uri);
                    if (file.getName().endsWith(".jar")) {
                        try (JarFile jarFile = new JarFile(file)) {
                            Enumeration<JarEntry> entries = jarFile.entries();
                            while (entries.hasMoreElements()) {
                                JarEntry jarEntry = entries.nextElement();
                                if (!jarEntry.getName().endsWith(".class")) {
                                    continue;
                                }
                                String className = jarEntry.getName().replace("/", ".");
                                if (packageName.isPresent() && !className.startsWith(packageName.get())) {
                                    continue;
                                }
                                className = className.substring(0, className.length() - 6);
                                try {
                                    Class<?> clazz = Class.forName(className, false, classLoader);
                                    if (clazz.isAnnotationPresent(AggregationFunction.class)
                                            || clazz.isAnnotationPresent(ScalarFunction.class)
                                            || clazz.isAnnotationPresent(ScalarOperator.class)
                                            || Arrays.stream(clazz.getMethods()).anyMatch(method -> method.isAnnotationPresent(ScalarFunction.class))) {
                                        functions.add(clazz);
                                    }
                                    log.info("Load udf class: " + className);
                                }
                                catch (ClassNotFoundException e) {
                                    throw new IOException(e);
                                }
                            }
                        }
                    }
                }
            }
        }
        catch (URISyntaxException e) {
            throw new UncheckedIOException(new IOException(e));
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return functions.build();
    }

    private Optional<File> downloadJars(TrinoFileSystemFactory fileSystemFactory, String user, String udfLocation)
    {
        boolean empty = true;
        File sourceFile = getCurrentClassLocation();
        String localDir = randomUUID().toString();
        File directory;
        if (sourceFile.isDirectory()) {
            // running DevelopmentServer in the IDE
            verify(sourceFile.getPath().endsWith("/target/classes"), "Source file not in 'target' directory: %s", sourceFile);
            directory = new File(sourceFile.getParentFile().getParentFile().getParentFile(), "trino-hdfs/target/" + localDir);
        }
        else {
            // normal server mode where HDFS JARs are in a subdirectory of the plugin
            directory = new File(sourceFile.getParentFile(), localDir);
        }

        try {
            TrinoFileSystem fileSystem = fileSystemFactory.create(ConnectorIdentity.ofUser(user));
            FileIterator fileIterator = fileSystem.listFiles(Location.of(udfLocation));
            while (fileIterator.hasNext()) {
                Location file = fileIterator.next().location();
                try (InputStream stream = fileSystem.newInputFile(file).newStream()) {
                    File jar = new File(directory, file.fileName());
                    FileUtils.copyToFile(stream, jar);
                }
                empty = false;
                log.info("Download jar file: " + file);
            }
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        if (empty) {
            return Optional.empty();
        }
        else {
            return Optional.of(directory);
        }
    }

    private File getCurrentClassLocation()
    {
        try {
            return new File(getClass().getProtectionDomain().getCodeSource().getLocation().toURI());
        }
        catch (URISyntaxException e) {
            throw new UncheckedIOException(new IOException(e));
        }
    }

    private static List<URL> buildClassPath(File path)
    {
        try (DirectoryStream<Path> directoryStream = newDirectoryStream(path.toPath())) {
            return stream(directoryStream)
                    .map(Path::toFile)
                    .sorted().toList().stream()
                    .map(UdfProviderConnector::fileToUrl)
                    .toList();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static URL fileToUrl(File file)
    {
        try {
            return file.toURI().toURL();
        }
        catch (MalformedURLException e) {
            throw new UncheckedIOException(e);
        }
    }
}
