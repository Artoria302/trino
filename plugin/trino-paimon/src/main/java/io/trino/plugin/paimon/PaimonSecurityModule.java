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
package io.trino.plugin.paimon;

import com.google.inject.Binder;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.trino.plugin.base.security.ConnectorAccessControlModule;
import io.trino.plugin.base.security.FileBasedAccessControlModule;
import io.trino.plugin.base.security.ReadOnlySecurityModule;

import static com.google.inject.util.Modules.EMPTY_MODULE;

public class PaimonSecurityModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        install(new ConnectorAccessControlModule());
        install(switch (buildConfigObject(PaimonSecurityConfig.class).getSecuritySystem()) {
            case ALLOW_ALL -> new AllowAllSecurityModule();
            case READ_ONLY -> new ReadOnlySecurityModule();
            case FILE -> new FileBasedAccessControlModule();
            // do not bind a ConnectorAccessControl so the engine will use system security with system roles
            case SYSTEM -> EMPTY_MODULE;
        });
    }
}
