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
package io.trino.plugin.exchange.filesystem.hdfs.authentication;

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.trino.plugin.base.authentication.KerberosAuthentication;
import io.trino.plugin.exchange.filesystem.hdfs.ExchangeHdfsConfig;
import io.trino.plugin.exchange.filesystem.hdfs.ExchangeHdfsConfig.HdfsAuthenticationType;

import javax.inject.Inject;

import java.util.function.Predicate;

import static com.google.inject.Scopes.SINGLETON;
import static io.airlift.configuration.ConditionalModule.conditionalModule;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.trino.plugin.exchange.filesystem.hdfs.authentication.KerberosHadoopAuthentication.createKerberosHadoopAuthentication;

public class HdfsAuthenticationModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        bindAuthenticationModule(
                HdfsAuthenticationModule::simpleHdfsAuth,
                simpleHdfsAuthenticationModule());

        bindAuthenticationModule(
                HdfsAuthenticationModule::kerberosHdfsAuth,
                kerberosHdfsAuthenticationModule());
    }

    private void bindAuthenticationModule(Predicate<ExchangeHdfsConfig> predicate, Module module)
    {
        install(conditionalModule(ExchangeHdfsConfig.class, predicate, module));
    }

    private static boolean simpleHdfsAuth(ExchangeHdfsConfig config)
    {
        return config.getHdfsAuthenticationType() == HdfsAuthenticationType.NONE;
    }

    private static boolean kerberosHdfsAuth(ExchangeHdfsConfig config)
    {
        return config.getHdfsAuthenticationType() == HdfsAuthenticationType.KERBEROS;
    }

    public static Module simpleHdfsAuthenticationModule()
    {
        return binder -> binder
                .bind(HdfsAuthentication.class)
                .to(SampleHdfsAuthentication.class)
                .in(SINGLETON);
    }

    public static Module kerberosHdfsAuthenticationModule()
    {
        return new Module()
        {
            @Override
            public void configure(Binder binder)
            {
                binder.bind(HdfsAuthentication.class)
                        .to(DirectHdfsAuthentication.class)
                        .in(SINGLETON);
                configBinder(binder).bindConfig(HdfsKerberosConfig.class);
            }

            @Inject
            @Provides
            @Singleton
            HadoopAuthentication createHadoopAuthentication(ExchangeHdfsConfig config, HdfsKerberosConfig hdfsKerberosConfig)
            {
                String principal = hdfsKerberosConfig.getHdfsPrincipal();
                String keytabLocation = hdfsKerberosConfig.getHdfsKeytab();
                KerberosAuthentication kerberosAuthentication = new KerberosAuthentication(principal, keytabLocation);
                KerberosHadoopAuthentication kerberosHadoopAuthentication = createKerberosHadoopAuthentication(kerberosAuthentication, config);
                return new CachingKerberosHadoopAuthentication(kerberosHadoopAuthentication);
            }
        };
    }
}
