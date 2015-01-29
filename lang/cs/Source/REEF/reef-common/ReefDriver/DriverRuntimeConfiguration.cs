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

using Org.Apache.Reef.Common;
using Org.Apache.Reef.Common.Api;
using Org.Apache.Reef.Common.Catalog;
using Org.Apache.Reef.Common.Client;
using Org.Apache.Reef.Common.Evaluator;
using Org.Apache.Reef.Driver.Evaluator;
using Org.Apache.Reef.Tang.Formats;
using Org.Apache.Reef.Tang.Util;

namespace Org.Apache.Reef.Driver
{
    public class DriverRuntimeConfiguration : ConfigurationModuleBuilder
    {
        public static ConfigurationModule ConfigurationModule
        {
            get
            {
                return new DriverRuntimeConfiguration()
                // Resource Catalog
                .BindImplementation(GenericType<IResourceCatalog>.Class, GenericType<ResourceCatalogImpl>.Class)

                // JobMessageObserver
                //.BindImplementation(GenericType<IEvaluatorRequestor>.Class, GenericType<DriverManager>.Class)
                .BindImplementation(GenericType<IJobMessageObserver>.Class, GenericType<ClientJobStatusHandler>.Class)

                // JobMessageObserver Wake event handler bindings
                .BindNamedParameter(GenericType<DriverRuntimeConfigurationOptions.JobMessageHandler>.Class, GenericType<ClientJobStatusHandler>.Class)
                .BindNamedParameter(GenericType<DriverRuntimeConfigurationOptions.JobExceptionHandler>.Class, GenericType<ClientJobStatusHandler>.Class)

                // Client manager
                .BindNamedParameter(GenericType<DriverRuntimeConfigurationOptions.JobControlHandler>.Class, GenericType<ClientManager>.Class)

                // Bind the runtime parameters
                //.BindNamedParameter(GenericType<RuntimeParameters.NodeDescriptorHandler>.Class, GenericType<DriverManager>.Class)
                //.BindNamedParameter(GenericType<RuntimeParameters.ResourceAllocationHandler>.Class, GenericType<DriverManager>.Class)
                //.BindNamedParameter(GenericType<RuntimeParameters.ResourceStatusHandler>.Class, GenericType<DriverManager>.Class)
                //.BindNamedParameter(GenericType<RuntimeParameters.RuntimeStatusHandler>.Class, GenericType<DriverManager>.Class)

                // Bind to the Clock
                //.BindSetEntry(GenericType<IClock.RuntimeStopHandler>.Class, GenericType<DriverManager>.Class)
                .Build();
            }
        }
    }
}
