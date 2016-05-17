// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
// 
//   http://www.apache.org/licenses/LICENSE-2.0
// 
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

using System;
using System.Globalization;
using System.IO;
using System.Threading.Tasks;
using Org.Apache.REEF.Client.Yarn.RestClient;
using Org.Apache.REEF.Client.YARN.RestClient;
using Org.Apache.REEF.Tang.Exceptions;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Tang.Util;
using Xunit;

namespace Org.Apache.REEF.Client.Tests
{
    [Collection("UrlProviderTests")]
    public class MultipleRMUrlProviderTests
    {
        private const string HadoopConfDirEnvVariable = "HADOOP_CONF_DIR";
        private const string YarnConfigFileName = "yarn-site.xml";
        private const string AnyHttpAddressConfig = @"anyhost:8088";
        private const string AnyHttpAddressConfigUpdated = @"anyhost1:8088";
        private const string AnyHttpsAddressConfig = @"anyotherhost:9088";

        private const string YarnConfigurationXmlContent = @"<?xml version=""1.0""?>
<?xml-stylesheet type=""text/xsl"" href=""configuration.xsl""?>
<!-- Put site-specific property overrides in this file. -->
<configuration xmlns:xi=""http://www.w3.org/2001/XInclude"">
  <property>
    <name>yarn.resourcemanager.ha.rm-ids</name>
    <value>rm1,rm2</value>
  </property>
  <property>
    <name>yarn.resourcemanager.webapp.address.rm1</name>
    <value>" + AnyHttpAddressConfig + @"</value>
  </property>
  <property>
    <name>yarn.resourcemanager.webapp.address.rm2</name>
    <value>" + AnyHttpAddressConfigUpdated + @"</value>
  </property>
  <property>
    <name>yarn.nodemanager.local-dirs</name>
    <value>C:\hdpdata\hadoop\local</value>
  </property>
</configuration>";

        [Fact]
        public void UrlProviderReadsEnvVarConfiguredConfigFileAndParsesCorrectHttpUrl()
        {
            string tempFile = Path.GetTempFileName();
            string tempDir = Path.GetDirectoryName(tempFile);
            string yarnConfigFile = Path.Combine(tempDir, YarnConfigFileName);

            using (new YarnConfigurationUrlProviderTests.TempFileWriter(yarnConfigFile, YarnConfigurationXmlContent))
            using (new YarnConfigurationUrlProviderTests.TemporaryOverrideEnvironmentVariable(HadoopConfDirEnvVariable, tempDir))
            {
                IUrlProvider urlProvider = GetYarnConfigurationUrlProvider();
                var url = urlProvider.GetUrlAsync().GetAwaiter().GetResult();

                int i = 0;
                foreach (var u in url)
                {
                    i++;
                    Assert.Equal("http", u.Scheme);
                    if (i == 1)
                    {
                        Assert.Equal(AnyHttpAddressConfig.Split(':')[0], u.Host);
                        Assert.Equal(AnyHttpAddressConfig.Split(':')[1],
                            u.Port.ToString(CultureInfo.InvariantCulture));
                    }
                    else
                    {
                        Assert.Equal(AnyHttpAddressConfigUpdated.Split(':')[0], u.Host);
                        Assert.Equal(AnyHttpAddressConfigUpdated.Split(':')[1],
                            u.Port.ToString(CultureInfo.InvariantCulture));
                    }
                }
            }
        }

        [Fact]
        public async Task CannotFindHadoopConfigDirThrowsArgumentException()
        {
            using (new YarnConfigurationUrlProviderTests.TemporaryOverrideEnvironmentVariable(HadoopConfDirEnvVariable, string.Empty))
            {
                var urlProvider = GetYarnConfigurationUrlProvider();
                await Assert.ThrowsAsync<ArgumentException>(async () => await urlProvider.GetUrlAsync());
            }
        }

        private static IUrlProvider GetYarnConfigurationUrlProvider()
        {
            var builder = TangFactory.GetTang().NewConfigurationBuilder()
                .BindImplementation(GenericType<IUrlProvider>.Class, GenericType<MultipleRMUrlProvider>.Class)
                .Build();

            return TangFactory.GetTang().NewInjector(builder).GetInstance<IUrlProvider>();
        }
    }
}
