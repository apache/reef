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

using System.Collections.Generic;
using System.Globalization;
using Org.Apache.REEF.Common.Context;
using Org.Apache.REEF.Common.Runtime.Evaluator.Utils;
using Org.Apache.REEF.Common.Services;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Formats;
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
        private static readonly Logger Logger = Logger.GetLogger(typeof(RootContextLauncher));

        private readonly IInjector _rootServiceInjector;
        private readonly IConfiguration _rootContextConfiguration;
        private ISet<object> _services;
        private ContextRuntime _rootContext;

        [Inject]
        private RootContextLauncher(
            AvroConfigurationSerializer serializer,
            [Parameter(typeof(RootContextConfiguration))] string rootContextConfiguration,
            [Parameter(typeof(RootServiceConfiguration))] string rootServiceConfiguration,
            IInjector injector)
            : this(serializer.FromString(rootContextConfiguration),
            serializer.FromString(rootServiceConfiguration), 
            Optional<IConfiguration>.Empty(),
            injector)
        {
        }

        [Inject]
        private RootContextLauncher(
            AvroConfigurationSerializer serializer,
            [Parameter(typeof(RootContextConfiguration))] string rootContextConfiguration,
            [Parameter(typeof(RootServiceConfiguration))] string rootServiceConfiguration,
            [Parameter(typeof(InitialTaskConfiguration))] string initialTaskConfiguration,
            IInjector injector)
            : this(serializer.FromString(rootContextConfiguration), 
            serializer.FromString(rootServiceConfiguration), 
            Optional<IConfiguration>.Of(serializer.FromString(initialTaskConfiguration)),
            injector)
        {
        }

        private RootContextLauncher(
            IConfiguration contextConfiguration,
            IConfiguration rootServiceConfig,
            Optional<IConfiguration> rootTaskConfig,
            IInjector injector)
        {
            _rootContextConfiguration = contextConfiguration;
            _rootServiceInjector = injector.ForkInjector(rootServiceConfig);
            Id = _rootServiceInjector
                .ForkInjector(contextConfiguration)
                .GetNamedInstance<ContextConfigurationOptions.ContextIdentifier, string>();
            _services = _rootServiceInjector.GetNamedInstance<ServicesSet, ISet<object>>();
            Logger.Log(Level.Verbose, string.Format(CultureInfo.InvariantCulture, "injected service(s)"));
            RootTaskConfig = rootTaskConfig;
        }

        public Optional<IConfiguration> RootTaskConfig { get; private set; }

        public string Id { get; private set; }

        internal ContextRuntime GetRootContext()
        {
            if (_rootContext == null)
            {
                _rootContext = new ContextRuntime(_rootServiceInjector, _rootContextConfiguration, Optional<ContextRuntime>.Empty());
            }

            return _rootContext;
        }
    }
}
