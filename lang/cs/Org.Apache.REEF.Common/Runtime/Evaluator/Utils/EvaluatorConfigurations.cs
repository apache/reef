/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

using System;
using System.IO;
using System.Linq;
using Org.Apache.REEF.Common.Runtime.Evaluator.Context;
using Org.Apache.REEF.Common.Services;
using Org.Apache.REEF.Common.Tasks;
using Org.Apache.REEF.Tang.Formats;
using Org.Apache.REEF.Tang.Formats.AvroConfigurationDataContract;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Utilities;
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.Common.Runtime.Evaluator.Utils
{
    public class EvaluatorConfigurations
    {
        private static readonly Logger LOGGER = Logger.GetLogger(typeof(EvaluatorConfigurations));

        private readonly AvroConfiguration _avroConfiguration;

        private readonly IConfiguration _evaluatorConfiguration;

        private readonly string _configFile;

        private string _applicationId;

        private string _evaluatorId;

        private string _taskConfiguration;

        private string _rootContextConfiguration;

        private string _rootServiceConfiguration;

        private string _errorHandlerRid;

        public EvaluatorConfigurations(string configFile)
        {
            using (LOGGER.LogFunction("EvaluatorConfigurations::EvaluatorConfigurations"))
            {
                if (string.IsNullOrWhiteSpace(configFile))
                {
                    Org.Apache.REEF.Utilities.Diagnostics.Exceptions.Throw(new ArgumentNullException("configFile"), LOGGER);
                }
                if (!File.Exists(configFile))
                {
                    Org.Apache.REEF.Utilities.Diagnostics.Exceptions.Throw(new FileNotFoundException("cannot find file " + configFile), LOGGER);
                }
                _configFile = configFile;
                AvroConfigurationSerializer serializer = new AvroConfigurationSerializer();
                _avroConfiguration = serializer.AvroDeserializeFromFile(_configFile);

                var language = _avroConfiguration.language;
                LOGGER.Log(Level.Info, "The language that created the configFile is " + language);

                var classHierarchy = TangFactory.GetTang()
                    .GetClassHierarchy(new string[] { typeof(ApplicationIdentifier).Assembly.GetName().Name });
                _evaluatorConfiguration = serializer.FromAvro(_avroConfiguration, classHierarchy);
            }
        }

        public string TaskConfigurationString
        {
            get
            {
                _taskConfiguration = _taskConfiguration ?? GetSettingValue(Constants.TaskConfiguration);
                return _taskConfiguration;
            }
        }

        /// <summary>
        /// The TaskConfiguration submitted with the evaluator configuration, if any.
        /// </summary>
        public Optional<TaskConfiguration> TaskConfiguration
        {
            get
            {
                var taskConfig = TaskConfigurationString;
                return string.IsNullOrEmpty(taskConfig)
                    ? Optional<TaskConfiguration>.Empty()
                    : Optional<TaskConfiguration>.Of(
                        new TaskConfiguration(taskConfig));
            }
        } 

        public string EvaluatorId
        {
            get
            {
                _evaluatorId = _evaluatorId ?? GetSettingValue(Constants.EvaluatorIdentifier);
                return _evaluatorId;
            }
        }

        public string ApplicationId
        {
            get
            {
                _applicationId = _applicationId ?? GetSettingValue(Constants.ApplicationIdentifier);
                return _applicationId;
            }
        }

        public string ErrorHandlerRID
        {
            get
            {
                _errorHandlerRid = _errorHandlerRid ?? GetSettingValue(Constants.ErrorHandlerRID);
                return _errorHandlerRid;
            }
        }

        public string RootContextConfigurationString
        {
            get
            {
                _rootContextConfiguration = _rootContextConfiguration ?? GetSettingValue(Constants.RootContextConfiguration);
                return _rootContextConfiguration;
            }
        }

        /// <summary>
        /// The ContextConfiguration for the root context.
        /// </summary>
        /// <exception cref="ArgumentException">If the underlying string parameter isn't set.</exception>
        public ContextConfiguration RootContextConfiguration
        {
            get
            {
                string rootContextConfigString = RootContextConfigurationString;
                if (string.IsNullOrWhiteSpace(rootContextConfigString))
                {
                    Utilities.Diagnostics.Exceptions.Throw(
                        new ArgumentException("empty or null rootContextConfigString"), LOGGER);
                }
                return new ContextConfiguration(rootContextConfigString);
            }
        }

        public string RootServiceConfigurationString
        {
            get
            {
                _rootServiceConfiguration = _rootServiceConfiguration ?? GetSettingValue(Constants.RootServiceConfiguration);
                return _rootServiceConfiguration;
            }
        }

        /// <summary>
        /// The ServiceConfiguration for the root context.
        /// </summary>
        /// <exception cref="ArgumentException">If the underlying string parameter isn't set.</exception>
        public Optional<ServiceConfiguration> RootServiceConfiguration
        {
            get
            {
                var rootServiceConfigString = RootServiceConfigurationString;
                return string.IsNullOrEmpty(rootServiceConfigString)
                    ? Optional<ServiceConfiguration>.Empty()
                    : Optional<ServiceConfiguration>.Of(
                        new ServiceConfiguration(
                            rootServiceConfigString));
            }
        } 

        private string GetSettingValue(string settingKey)
        {
            ConfigurationEntry configurationEntry =
                _avroConfiguration.Bindings.SingleOrDefault(b => b.key.EndsWith(settingKey, StringComparison.OrdinalIgnoreCase));
            if (configurationEntry == null)
            {
                return string.Empty;
            }

            return configurationEntry.value;
        }
    }
}
