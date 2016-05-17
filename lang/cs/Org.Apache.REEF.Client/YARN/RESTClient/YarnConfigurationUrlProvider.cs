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
using System.Threading.Tasks;
using Org.Apache.REEF.Tang.Annotations;

namespace Org.Apache.REEF.Client.Yarn.RestClient
{
    /// <summary>
    /// The <see cref="IUrlProvider"/> for YARN that reads yarn-site.xml to locate RM web 
    /// URLs for client job submission.
    /// </summary>
    public sealed class YarnConfigurationUrlProvider : IUrlProvider
    {
        private readonly string _hadoopConfigDir;
        private readonly bool _useHttps;

        /// <summary>
        /// The location of Hadoop configurations. Defaults to a whitespace string,
        /// which instructs <see cref="YarnConfigurationUrlProvider"/> to read
        /// the environment variable HADOOP_CONF_DIR.
        /// </summary>
        [NamedParameter("Location of Hadoop configurations.", defaultValue: " ")]
        public sealed class HadoopConfigurationDirectory : Name<string>
        {
            private HadoopConfigurationDirectory()
            {
            }
        }

        /// <summary>
        /// Whether the provider should use HTTPS to construct the URI or not.
        /// </summary>
        [NamedParameter("True if HTTPS should be used to construct the URI.", defaultValue: "false")]
        public sealed class UseHttpsForYarnCommunication : Name<bool>
        {
            private UseHttpsForYarnCommunication()
            {
            }
        }

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
            _hadoopConfigDir = hadoopConfigDir;
            _useHttps = useHttps;
        }

        public Task<IEnumerable<Uri>> GetUrlAsync()
        {
            return Task.FromResult(Utilities.Yarn.GetYarnRMWebappEndpoints(_hadoopConfigDir, _useHttps));
        }
    }
}