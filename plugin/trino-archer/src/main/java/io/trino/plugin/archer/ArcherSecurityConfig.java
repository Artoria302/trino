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

import io.airlift.configuration.Config;
import jakarta.validation.constraints.NotNull;

public class ArcherSecurityConfig
{
    public enum ArcherSecurity
    {
        ALLOW_ALL,
        READ_ONLY,
        SYSTEM,
        FILE,
    }

    private ArcherSecurity securitySystem = ArcherSecurity.ALLOW_ALL;

    @NotNull
    public ArcherSecurityConfig.ArcherSecurity getSecuritySystem()
    {
        return securitySystem;
    }

    @Config("archer.security")
    public ArcherSecurityConfig setSecuritySystem(ArcherSecurity securitySystem)
    {
        this.securitySystem = securitySystem;
        return this;
    }
}
