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

import io.trino.hadoop.HadoopNative;
import io.trino.plugin.base.authentication.KerberosAuthentication;
import io.trino.plugin.exchange.filesystem.hdfs.ExchangeHdfsConfig;
import io.trino.plugin.exchange.filesystem.hdfs.HdfsConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;

import javax.security.auth.Subject;

import static java.util.Objects.requireNonNull;
import static org.apache.hadoop.security.UserGroupInformationShim.createUserGroupInformationForSubject;

public class KerberosHadoopAuthentication
        implements HadoopAuthentication
{
    private final KerberosAuthentication kerberosAuthentication;

    public static KerberosHadoopAuthentication createKerberosHadoopAuthentication(KerberosAuthentication kerberosAuthentication, ExchangeHdfsConfig config)
    {
        // Load native libraries, which are required during initialization of UserGroupInformation
        HadoopNative.requireHadoopNative();

        Configuration configuration = HdfsConfiguration.getConfiguration(config);

        // In order to enable KERBEROS authentication method for HDFS
        // UserGroupInformation.authenticationMethod static field must be set to KERBEROS
        // It is further used in many places in DfsClient
        configuration.set("hadoop.security.authentication", "kerberos");

        UserGroupInformation.setConfiguration(configuration);

        return new KerberosHadoopAuthentication(kerberosAuthentication);
    }

    private KerberosHadoopAuthentication(KerberosAuthentication kerberosAuthentication)
    {
        this.kerberosAuthentication = requireNonNull(kerberosAuthentication, "kerberosAuthentication is null");
    }

    @Override
    public UserGroupInformation getUserGroupInformation()
    {
        Subject subject = kerberosAuthentication.getSubject();
        return createUserGroupInformationForSubject(subject);
    }
}
