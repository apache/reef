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
using System.Globalization;
using System.IO;
using Org.Apache.REEF.Tang.Formats.AvroConfigurationDataContract;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Tang.Types;
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.Common.Runtime.Evaluator.Context
{
    // TODO[JIRA REEF-1167]: Remove class.
    [Obsolete("Deprecated in 0.14. Will be removed.")]
    internal sealed class ContextConfiguration : IConfiguration
    {
        private static readonly Logger LOGGER = Logger.GetLogger(typeof(ContextConfiguration));
        
        private readonly Dictionary<string, string> _settings;

        public ContextConfiguration(string configString)
        {
            using (LOGGER.LogFunction("ContextConfiguration::ContextConfigurationn"))
            {
                ContainerDirectory = Directory.GetCurrentDirectory();

                _settings = new Dictionary<string, string>();
                AvroConfiguration avroConfiguration = AvroConfiguration.GetAvroConfigurationFromEmbeddedString(configString);
                foreach (ConfigurationEntry config in avroConfiguration.Bindings)
                {
                    if (config.key.Contains(Constants.ContextIdentifier))
                    {
                        config.key = Constants.ContextIdentifier;
                        LOGGER.Log(Level.Verbose, string.Format(CultureInfo.InvariantCulture, "{0} detected for context id with value {1}", config.key, config.value));
                    }
                    _settings.Add(config.key, config.value);
                }
                if (!_settings.ContainsKey(Constants.ContextIdentifier))
                {
                    string msg = "Required parameter ContextIdentifier not provided.";
                    LOGGER.Log(Level.Error, msg);
                    Utilities.Diagnostics.Exceptions.Throw(new ArgumentException(msg), LOGGER);
                }
            }
        }

        public string Id
        {
            get { return _settings[Constants.ContextIdentifier]; }
        }

        public string ContainerDirectory { get; set; }

        public IConfigurationBuilder newBuilder()
        {
            throw new NotImplementedException();
        }

        public string GetNamedParameter(INamedParameterNode np)
        {
            throw new NotImplementedException();
        }

        public IClassHierarchy GetClassHierarchy()
        {
            throw new NotImplementedException();
        }

        public ISet<object> GetBoundSet(INamedParameterNode np)
        {
            throw new NotImplementedException();
        }

        public IClassNode GetBoundConstructor(IClassNode cn)
        {
            throw new NotImplementedException();
        }

        public IClassNode GetBoundImplementation(IClassNode cn)
        {
            throw new NotImplementedException();
        }

        public IConstructorDef GetLegacyConstructor(IClassNode cn)
        {
            throw new NotImplementedException();
        }

        public ICollection<IClassNode> GetBoundImplementations()
        {
            throw new NotImplementedException();
        }

        public ICollection<IClassNode> GetBoundConstructors()
        {
            throw new NotImplementedException();
        }

        public ICollection<INamedParameterNode> GetNamedParameters()
        {
            throw new NotImplementedException();
        }

        public ICollection<IClassNode> GetLegacyConstructors()
        {
            throw new NotImplementedException();
        }

        public IList<object> GetBoundList(INamedParameterNode np)
        {
            throw new NotImplementedException();
        }

        public IEnumerator<KeyValuePair<INamedParameterNode, object>> GetBoundSets()
        {
            throw new NotImplementedException();
        }

        public IDictionary<INamedParameterNode, IList<object>> GetBoundList()
        {
            throw new NotImplementedException();
        }
    }
}
