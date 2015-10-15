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
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using System.Xml.Linq;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.Client.Yarn.RestClient
{
    internal class YarnConfigurationUrlProvider : IUrlProvider
    {
        [NamedParameter("Location of Hadoop configurations.")]
        public class HadoopConfigurationDirectory : Name<string>
        {
        }

        [NamedParameter("True if HTTPS should be used to construct the URI.", defaultValue: "false")]
        public class UseHttpsForYarnCommunication : Name<bool>
        {
        }

        private static readonly string HadoopConfDirEnvVariable = "HADOOP_CONF_DIR";
        private static readonly string YarnConfigFileName = "yarn-site.xml";
        private static readonly string YarnRmWebappHttpsAddressPropertyName = "yarn.resourcemanager.webapp.https.address";
        private static readonly string YarnRmWebappHttpAddressPropertyName = "yarn.resourcemanager.webapp.address";
        private static readonly Logger Logger = Logger.GetLogger(typeof(YarnConfigurationUrlProvider));
        private Uri _yarnRmUri;

        [Inject]
        private YarnConfigurationUrlProvider(
            [Parameter(typeof(UseHttpsForYarnCommunication))] bool useHttps)
            : this(null, useHttps)
        {
        }

        [Inject]
        private YarnConfigurationUrlProvider(
            [Parameter(typeof(HadoopConfigurationDirectory))] string hadoopConfigDir,
            [Parameter(typeof(UseHttpsForYarnCommunication))] bool useHttps)
        {
            hadoopConfigDir = hadoopConfigDir ?? Environment.GetEnvironmentVariable(HadoopConfDirEnvVariable);

            if (string.IsNullOrEmpty(hadoopConfigDir) || !Directory.Exists(hadoopConfigDir))
            {
                throw new ArgumentException(HadoopConfDirEnvVariable + " is not configured or does not exist.",
                    "hadoopConfigDir");
            }

            Logger.Log(Level.Verbose, "Using {0} as hadoop configuration directory", hadoopConfigDir);
            string yarnConfigurationFile = Path.Combine(hadoopConfigDir, YarnConfigFileName);
            LoadYarnConfiguration(yarnConfigurationFile, useHttps);
        }

        public Task<Uri> GetUrlAsync()
        {
            return Task.FromResult(_yarnRmUri);
        }

        private void LoadYarnConfiguration(string yarnConfigurationFile, bool useHttps)
        {
            var configRoot = XElement.Load(yarnConfigurationFile);

            string propertyName = useHttps
                ? YarnRmWebappHttpsAddressPropertyName
                : YarnRmWebappHttpAddressPropertyName;

            string prefix = useHttps ? "https" : "http";

            var address = (string)configRoot.Elements("property")
                .Where(x =>
                    (string)x.Element("name") == propertyName)
                .Select(x => x.Element("value")).FirstOrDefault();

            address = address.TrimEnd('/') + @"/";

            _yarnRmUri = new Uri(string.Format("{0}://{1}", prefix, address));
        }
    }
}