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

import com.google.common.collect.ImmutableList;

import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Enumeration;
import java.util.List;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.lang.System.identityHashCode;
import static java.util.Objects.requireNonNull;

public class UdfProviderClassLoader
        extends URLClassLoader
{
    private static final ImmutableList<String> SPI_PACKAGES = ImmutableList.<String>builder()
            .add("io.trino.spi.")
            .add("com.fasterxml.jackson.annotation.")
            .add("io.airlift.slice.")
            .add("org.openjdk.jol.")
            .add("io.opentelemetry.api.")
            .add("io.opentelemetry.context.")
            .build();

    private static final ImmutableList<String> SPI_RESOURCES = SPI_PACKAGES.stream()
            .map(UdfProviderClassLoader::classNameToResource)
            .collect(toImmutableList());

    private final ClassLoader parentClassLoader;

    public UdfProviderClassLoader(
            List<URL> urls,
            ClassLoader parentClassLoader)
    {
        // plugins should not have access to the system (application) class loader
        super(urls.toArray(new URL[0]), getPlatformClassLoader());
        this.parentClassLoader = requireNonNull(parentClassLoader, "spiClassLoader is null");
    }

    public UdfProviderClassLoader withUrl(URL url)
    {
        List<URL> urls = ImmutableList.<URL>builder().add(getURLs()).add(url).build();
        return new UdfProviderClassLoader(urls, parentClassLoader);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .omitNullValues()
                .add("identityHash", "@" + Integer.toHexString(identityHashCode(this)))
                .toString();
    }

    @Override
    protected Class<?> loadClass(String name, boolean resolve)
            throws ClassNotFoundException
    {
        // grab the magic lock
        synchronized (getClassLoadingLock(name)) {
            // Check if class is in the loaded classes cache
            Class<?> cachedClass = findLoadedClass(name);
            if (cachedClass != null) {
                return resolveClass(cachedClass, resolve);
            }

            // If this is an SPI class, only check SPI class loader
            if (isSpiClass(name)) {
                return resolveClass(parentClassLoader.loadClass(name), resolve);
            }

            // Look for class locally
            return super.loadClass(name, resolve);
        }
    }

    private Class<?> resolveClass(Class<?> clazz, boolean resolve)
    {
        if (resolve) {
            resolveClass(clazz);
        }
        return clazz;
    }

    @Override
    public URL getResource(String name)
    {
        // If this is an SPI resource, only check SPI class loader
        if (isSpiResource(name)) {
            return parentClassLoader.getResource(name);
        }

        // Look for resource locally
        return super.getResource(name);
    }

    @Override
    public Enumeration<URL> getResources(String name)
            throws IOException
    {
        // If this is an SPI resource, use SPI resources
        if (isSpiClass(name)) {
            return parentClassLoader.getResources(name);
        }

        // Use local resources
        return super.getResources(name);
    }

    private boolean isSpiClass(String name)
    {
        // todo maybe make this more precise and only match base package
        return SPI_PACKAGES.stream().anyMatch(name::startsWith);
    }

    private boolean isSpiResource(String name)
    {
        // todo maybe make this more precise and only match base package
        return SPI_RESOURCES.stream().anyMatch(name::startsWith);
    }

    private static String classNameToResource(String className)
    {
        return className.replace('.', '/');
    }
}
