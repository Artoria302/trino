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

import io.airlift.configuration.Config;
import jakarta.validation.constraints.NotNull;

import java.util.Optional;

public class UdfProviderConfig
{
    private Optional<String> location = Optional.empty();
    private String user;
    private Optional<String> packageName = Optional.empty();

    public Optional<String> getLocation()
    {
        return location;
    }

    @Config("udf.fs.location")
    public UdfProviderConfig setLocation(String location)
    {
        this.location = Optional.of(location);
        return this;
    }

    @NotNull
    public String getUser()
    {
        return user;
    }

    @Config("udf.fs.user")
    public UdfProviderConfig setUser(String user)
    {
        this.user = user;
        return this;
    }

    @NotNull
    public Optional<String> getPackage()
    {
        return packageName;
    }

    @Config("udf.package-prefix")
    public UdfProviderConfig setPackage(String packageName)
    {
        this.packageName = Optional.of(packageName);
        return this;
    }
}
