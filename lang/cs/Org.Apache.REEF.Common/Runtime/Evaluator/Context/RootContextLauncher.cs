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
using Org.Apache.REEF.Common.Services;
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
        private static readonly Logger Logger = Logger.GetLogger(typeof(RootContextLauncher));

        private readonly IInjector _rootServiceInjector;
        private readonly IConfiguration _rootContextConfiguration;
        private ISet<object> _services;
        private ContextRuntime _rootContext;

        public RootContextLauncher(string id, IConfiguration contextConfiguration,
            Optional<IConfiguration> rootServiceConfig, Optional<IConfiguration> rootTaskConfig, IHeartBeatManager heartbeatManager)
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
                _rootContext = new ContextRuntime(_rootServiceInjector, _rootContextConfiguration, Optional<ContextRuntime>.Empty());
            }

            return _rootContext;
        }

        private IInjector InjectServices(Optional<IConfiguration> serviceConfig)
        {
            // TODO[JIRA REEF-217]: Use base injector for the Evaluator here instead.
            IInjector rootServiceInjector;

            if (serviceConfig.IsPresent())
            {
                rootServiceInjector = TangFactory.GetTang().NewInjector(serviceConfig.Value);
                try
                {
                    _services = rootServiceInjector.GetNamedInstance<ServicesSet, ISet<object>>();
                    Logger.Log(Level.Info, string.Format(CultureInfo.InvariantCulture, "injected service(s)"));
                }
                catch (Exception e)
                {
                    var errorMessage = string.Format(CultureInfo.InvariantCulture,
                        "Failed to inject service: encountered error {1} with message [{0}] and stack trace:[{2}]", e,
                        e.Message, e.StackTrace);
                    Utilities.Diagnostics.Exceptions.Caught(e, Level.Error, "Failed to instantiate service.", Logger);
                    var ex = new InvalidOperationException(errorMessage, e);
                    Utilities.Diagnostics.Exceptions.Throw(ex, Logger);
                }
            }
            else
            {
                rootServiceInjector = TangFactory.GetTang().NewInjector();
                Logger.Log(Level.Info, "no service provided for injection.");
            }
            
            return rootServiceInjector;
        }
    }
}
