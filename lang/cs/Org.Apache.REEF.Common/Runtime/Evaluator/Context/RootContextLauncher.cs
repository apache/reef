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
using Org.Apache.REEF.Common.Services;
using Org.Apache.REEF.Common.Tasks;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Tang.Util;
using Org.Apache.REEF.Utilities;
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.Common.Runtime.Evaluator.Context
{
    /// <summary>
    ///  Helper class that encapsulates the root context configuration: With or without services and an initial task.
    /// </summary>
    internal sealed class RootContextLauncher
    {
        private static readonly Logger LOGGER = Logger.GetLogger(typeof(RootContextLauncher));
        
        private readonly IInjector _rootServiceInjector = null;

        private readonly IConfiguration _rootContextConfiguration;

        private ContextRuntime _rootContext = null;

        public RootContextLauncher(string id, IConfiguration contextConfiguration,
            Optional<ServiceConfiguration> rootServiceConfig, Optional<IConfiguration> rootTaskConfig, IHeartBeatManager heartbeatManager)
        {
            Id = id;
            _rootContextConfiguration = contextConfiguration;
            _rootServiceInjector = InjectServices(rootServiceConfig);
            _rootServiceInjector.BindVolatileInstance(GenericType<IHeartBeatManager>.Class, heartbeatManager);
            RootTaskConfig = rootTaskConfig;
        }

        public Optional<IConfiguration> RootTaskConfig { get; set; }

        public string Id { get; private set; }

        internal ContextRuntime GetRootContext()
        {
            if (_rootContext == null)
            {
                // TODO[JIRA REEF-1167]: Remove use of deprecated ContextRuntime constructor and deprecatedContextConfiguration
                var deprecatedContextConfiguration = _rootContextConfiguration as ContextConfiguration;
                if (deprecatedContextConfiguration != null)
                {
                    LOGGER.Log(Level.Info, "Using deprecated ContextConfiguration.");
                    _rootContext = new ContextRuntime(Id, _rootServiceInjector, _rootContextConfiguration);
                }
                else
                {
                    _rootContext = new ContextRuntime(_rootServiceInjector, _rootContextConfiguration, Optional<ContextRuntime>.Empty());
                }
            }
            return _rootContext;
        }

        private static IInjector InjectServices(Optional<ServiceConfiguration> serviceConfig)
        {
            // TODO[JIRA REEF-217]: Use base injector for the Evaluator here instead.
            IInjector rootServiceInjector;

            if (serviceConfig.IsPresent())
            {
                rootServiceInjector = TangFactory.GetTang().NewInjector(serviceConfig.Value.TangConfig);
                InjectedServices services = null;
                try
                {
                    services = rootServiceInjector.GetInstance<InjectedServices>();
                }
                catch (Exception e)
                {
                    Utilities.Diagnostics.Exceptions.Caught(e, Level.Error, "Failed to instantiate service.", LOGGER);
                    InvalidOperationException ex = new InvalidOperationException(string.Format(CultureInfo.InvariantCulture, "Failed to inject service: encoutned error {1} with message [{0}] and stack trace:[{1}]", e, e.Message, e.StackTrace));
                    Utilities.Diagnostics.Exceptions.Throw(ex, LOGGER);
                }
                LOGGER.Log(Level.Info, string.Format(CultureInfo.InvariantCulture, "injected {0} service(s)", services.Services.Count));
            }
            else
            {
                rootServiceInjector = TangFactory.GetTang().NewInjector();
                LOGGER.Log(Level.Info, "no service provided for injection.");
            }
            
            return rootServiceInjector;
        }
    }
}
