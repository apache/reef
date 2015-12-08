/**
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
using Org.Apache.REEF.Common.Services;
using Org.Apache.REEF.Common.Tasks;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Tang.Interface;
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

        private ContextRuntime _rootContext = null;

        private ContextConfiguration _rootContextConfiguration = null;

        public RootContextLauncher(ContextConfiguration rootContextConfig, Optional<ServiceConfiguration> rootServiceConfig, Optional<TaskConfiguration> rootTaskConfig)
        {
            _rootContextConfiguration = rootContextConfig;
            _rootServiceInjector = InjectServices(rootServiceConfig);
            RootTaskConfig = rootTaskConfig;
        }

        public Optional<TaskConfiguration> RootTaskConfig { get; set; }

        public ContextConfiguration RootContextConfig
        {
            get { return _rootContextConfiguration; }
            set { _rootContextConfiguration = value; }
        }

        internal ContextRuntime GetRootContext()
        {
            if (_rootContext == null)
            {
                _rootContext = GetRootContext(_rootServiceInjector, _rootContextConfiguration);
            }
            return _rootContext;
        }

        private IInjector InjectServices(Optional<ServiceConfiguration> serviceConfig)
        {
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
                    Org.Apache.REEF.Utilities.Diagnostics.Exceptions.Caught(e, Level.Error, "Failed to instantiate service.", LOGGER);
                    InvalidOperationException ex = new InvalidOperationException(string.Format(CultureInfo.InvariantCulture, "Failed to inject service: encoutned error {1} with message [{0}] and stack trace:[{1}]", e, e.Message, e.StackTrace));
                    Org.Apache.REEF.Utilities.Diagnostics.Exceptions.Throw(ex, LOGGER);
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

        private ContextRuntime GetRootContext( 
            IInjector rootServiceInjector,
            IConfiguration rootContextConfiguration)
        {
            ContextRuntime result;
            result = new ContextRuntime(rootServiceInjector, rootContextConfiguration);
            return result;
        }
    }
}
////if (rootServiceInjector != null)
////{
////   try
////   {
////       rootServiceInjector = rootServiceInjector.ForkInjector(serviceConfigs);
////   }
////   catch (Exception e)
////   {
////       throw new ContextClientCodeException(ContextClientCodeException.GetId(rootContextConfiguration),
////                                            Optional<String>.Empty(),
////                                            "Unable to instatiate the root context", e);
////   }
////   result = new ContextRuntime(rootServiceInjector, rootContextConfiguration);
////}
////else
////{
////   result = new ContextRuntime(rootServiceInjector.ForkInjector(), rootContextConfiguration);
////}