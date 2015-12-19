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
using System.Globalization;
using System.IO;
using Org.Apache.REEF.Common.Runtime.Evaluator.Context;
using Org.Apache.REEF.Common.Services;
using Org.Apache.REEF.Common.Tasks;
using Org.Apache.REEF.Tang.Exceptions;
using Org.Apache.REEF.Tang.Formats;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Utilities;
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.Common.Runtime.Evaluator.Utils
{
    internal sealed class EvaluatorConfigurations
    {
        private static readonly Logger LOGGER = Logger.GetLogger(typeof(EvaluatorConfigurations));

        private readonly string _applicationId;

        private readonly string _evaluatorId;

        private readonly string _taskConfiguration;

        private readonly string _rootContextConfiguration;

        private readonly string _rootServiceConfiguration;

        private readonly string _errorHandlerRid;

        private readonly string _remoteId;

        private readonly string _launchId;

        public EvaluatorConfigurations(string configFile)
        {
            using (LOGGER.LogFunction("EvaluatorConfigurations::EvaluatorConfigurations"))
            {
                if (string.IsNullOrWhiteSpace(configFile))
                {
                    Utilities.Diagnostics.Exceptions.Throw(new ArgumentNullException("configFile"), LOGGER);
                }
                if (!File.Exists(configFile))
                {
                    Utilities.Diagnostics.Exceptions.Throw(new FileNotFoundException("cannot find file " + configFile), LOGGER);
                }

                AvroConfigurationSerializer serializer = new AvroConfigurationSerializer();
                var classHierarchy = TangFactory.GetTang()
                    .GetClassHierarchy(new string[] { typeof(ApplicationIdentifier).Assembly.GetName().Name });
                var evaluatorConfiguration = serializer.FromFile(configFile, classHierarchy);

                IInjector evaluatorInjector = TangFactory.GetTang().NewInjector(evaluatorConfiguration);

                LOGGER.Log(Level.Info, 
                    string.Format(CultureInfo.CurrentCulture, "Evaluator Configuration is deserialized from file {0}:", configFile));
                try
                {
                    _taskConfiguration = evaluatorInjector.GetNamedInstance<InitialTaskConfiguration, string>();
                }
                catch (InjectionException)
                {
                    LOGGER.Log(Level.Info, "InitialTaskConfiguration is not set in Evaluator.config.");
                }

                try
                {
                    _rootContextConfiguration = evaluatorInjector.GetNamedInstance<RootContextConfiguration, string>();
                }
                catch (InjectionException)
                {
                    LOGGER.Log(Level.Warning, "RootContextConfiguration is not set in Evaluator.config.");
                }

                try
                {
                    _rootServiceConfiguration = evaluatorInjector.GetNamedInstance<RootServiceConfiguration, string>();
                }
                catch (InjectionException)
                {
                    LOGGER.Log(Level.Info, "RootServiceConfiguration is not set in Evaluator.config.");
                }

                _applicationId = evaluatorInjector.GetNamedInstance<ApplicationIdentifier, string>();
                _remoteId = evaluatorInjector.GetNamedInstance<DriverRemoteIdentifier, string>();
                _evaluatorId = evaluatorInjector.GetNamedInstance<EvaluatorIdentifier, string>();
                _errorHandlerRid = evaluatorInjector.GetNamedInstance<ErrorHandlerRid, string>();
                _launchId = evaluatorInjector.GetNamedInstance<LaunchId, string>();
            }
        }

        public string TaskConfigurationString
        {
            get
            {
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
                return _evaluatorId;
            }
        }

        public string ApplicationId
        {
            get
            {
                return _applicationId;
            }
        }

        public string ErrorHandlerRid
        {
            get
            {
                return _errorHandlerRid;
            }
        }

        public string RemoteId
        {
            get
            {
                return _remoteId;
            }
        }

        public string LaunchId
        {
            get
            {
                return _launchId;
            }
        }

        public string RootContextConfigurationString
        {
            get
            {
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
    }
}
