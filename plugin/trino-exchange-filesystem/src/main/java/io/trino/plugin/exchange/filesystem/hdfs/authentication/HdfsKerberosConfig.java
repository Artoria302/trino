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

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;

import javax.validation.constraints.NotNull;

public class HdfsKerberosConfig
{
    private String hdfsPrincipal;
    private String hdfsKeytab;

    @NotNull
    public String getHdfsPrincipal()
    {
        return hdfsPrincipal;
    }

    @Config("exchange.hdfs.krb5.principal")
    @ConfigDescription("HDFS kerberos principal")
    public HdfsKerberosConfig setHdfsPrincipal(String principal)
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
    public HdfsKerberosConfig setHdfsKeytab(String keytab)
    {
        this.hdfsKeytab = keytab;
        return this;
    }
}
