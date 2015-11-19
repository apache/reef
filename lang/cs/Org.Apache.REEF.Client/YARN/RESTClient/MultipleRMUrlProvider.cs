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
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using System.Xml.Linq;
using Org.Apache.REEF.Client.Yarn.RestClient;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.Client.YARN.RestClient
{
    public class MultipleRMUrlProvider : IUrlProvider
    {
        private const string RmConfigKeyPrefix = "yarn.resourcemanager.webapp.address.rm";
        private static readonly string HadoopConfDirEnvVariable = "HADOOP_CONF_DIR";
        private static readonly string YarnConfigFileName = "yarn-site.xml";
        private static readonly Logger Logger = Logger.GetLogger(typeof(MultipleRMUrlProvider));
        private IList<Uri> _yarnRmUri;

        [Inject]
        private MultipleRMUrlProvider()
        {
            var hadoopConfigDir = Environment.GetEnvironmentVariable(HadoopConfDirEnvVariable);

            if (string.IsNullOrEmpty(hadoopConfigDir) || !Directory.Exists(hadoopConfigDir))
            {
                throw new ArgumentException(HadoopConfDirEnvVariable + " is not configured or does not exist.",
                    "hadoopConfigDir");
            }

            Logger.Log(Level.Verbose, "Using {0} as hadoop configuration directory", hadoopConfigDir);
            string yarnConfigurationFile = Path.Combine(hadoopConfigDir, YarnConfigFileName);
            LoadYarnConfiguration(yarnConfigurationFile);
        }

        public Task<IEnumerable<Uri>> GetUrlAsync()
        {
            return Task.FromResult((IEnumerable<Uri>)_yarnRmUri);
        }

        private void LoadYarnConfiguration(string yarnConfigurationFile)
        {
            var configRoot = XElement.Load(yarnConfigurationFile);
            var address = configRoot.Elements("property")
                .Where(x =>
                    ((string)x.Element("name")).ToUpper().StartsWith(RmConfigKeyPrefix.ToUpper()))
                .Select(x => (string)x.Element("value"));
            _yarnRmUri =
                address.Select(x => x.TrimEnd('/') + @"/")
                    .Select(x => string.Format("http://{0}", x))
                    .Where(x => Uri.IsWellFormedUriString(x, UriKind.Absolute))
                    .Select(x => new Uri(x))
                    .ToList();
        }
    }
}