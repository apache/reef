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
using System.Linq;
using System.Net;
using Org.Apache.REEF.Common.Io;
using Org.Apache.REEF.Common.Services;
using Org.Apache.REEF.Common.Tasks;
using Org.Apache.REEF.Driver.Bridge;
using Org.Apache.REEF.Driver.Evaluator;
using Org.Apache.REEF.Examples.Tasks.HelloTask;
using Org.Apache.REEF.Network.Naming;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Implementations.Configuration;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Tang.Util;
using Org.Apache.REEF.Utilities;
using Org.Apache.REEF.Common.Context;

namespace Org.Apache.REEF.Examples.AllHandlers
{
    /// <summary>
    /// A sample implementation of allocatedEvaluator handler
    /// </summary>
    public class HelloAllocatedEvaluatorHandler : IObserver<IAllocatedEvaluator>
    {
        [Inject]
        private HelloAllocatedEvaluatorHandler()
        {
        }

        /// <summary>
        /// This method create Service/context/task configuration and submit them to the allocatedEvaluator
        /// </summary>
        /// <param name="allocatedEvaluator"></param>
        public void OnNext(IAllocatedEvaluator allocatedEvaluator)
        {
            string control = string.Empty;

            ISet<string> arguments = ClrHandlerHelper.GetCommandLineArguments();

            if (arguments != null && arguments.Any())
            {
                foreach (string argument in arguments)
                {
                    Console.WriteLine("testing argument: " + argument);
                }

                control = arguments.Last();
            }

            IEvaluatorDescriptor descriptor = allocatedEvaluator.GetEvaluatorDescriptor();

            IConfiguration serviceConfiguration = ServiceConfiguration.ConfigurationModule
                .Set(ServiceConfiguration.Services, GenericType<HelloService>.Class)
                .Build();

            IConfiguration contextConfiguration = ContextConfiguration.ConfigurationModule
                .Set(ContextConfiguration.Identifier, "bridgeHelloCLRContextId_" + Guid.NewGuid().ToString("N"))
                .Build();

            IConfiguration taskConfiguration = TaskConfiguration.ConfigurationModule
                .Set(TaskConfiguration.Identifier, "bridgeHelloCLRTaskId_" + Guid.NewGuid().ToString("N"))
                .Set(TaskConfiguration.Task, GenericType<HelloTask>.Class)
                .Set(TaskConfiguration.OnMessage, GenericType<HelloTask.HelloDriverMessageHandler>.Class)
                .Set(TaskConfiguration.OnSendMessage, GenericType<HelloTaskMessage>.Class)
                .Build();

            IConfiguration mergedTaskConfiguration = taskConfiguration;

            if (allocatedEvaluator.NameServerInfo != null)
            {
                IPEndPoint nameServerEndpoint = NetUtilities.ParseIpEndpoint(allocatedEvaluator.NameServerInfo);

                IConfiguration nameClientConfiguration = TangFactory.GetTang().NewConfigurationBuilder(
                    NamingConfiguration.ConfigurationModule
                                       .Set(NamingConfiguration.NameServerAddress, nameServerEndpoint.Address.ToString())
                                       .Set(NamingConfiguration.NameServerPort,
                                            nameServerEndpoint.Port.ToString(CultureInfo.InvariantCulture))
                                       .Build())
                                                                    .BindImplementation(GenericType<INameClient>.Class,
                                                                                        GenericType<NameClient>.Class)
                                                                    .Build();

                mergedTaskConfiguration = Configurations.Merge(taskConfiguration, nameClientConfiguration);
            }

            string ipAddress = descriptor.NodeDescriptor.InetSocketAddress.Address.ToString();
            int port = descriptor.NodeDescriptor.InetSocketAddress.Port;
            string hostName = descriptor.NodeDescriptor.HostName;
            Console.WriteLine(string.Format(CultureInfo.InvariantCulture, "Alloated evaluator {0} with ip {1}:{2}. Hostname is {3}", allocatedEvaluator.Id, ipAddress, port, hostName));
            Console.WriteLine(string.Format(CultureInfo.InvariantCulture, "Evaluator is assigned with {0} MB of memory and {1} cores.", descriptor.Memory, descriptor.VirtualCore));

            if (control.Equals("submitContext", StringComparison.OrdinalIgnoreCase))
            {
                allocatedEvaluator.SubmitContext(contextConfiguration);
            }
            else if (control.Equals("submitContextAndServiceAndTask", StringComparison.OrdinalIgnoreCase))
            {
                allocatedEvaluator.SubmitContextAndServiceAndTask(contextConfiguration, serviceConfiguration, mergedTaskConfiguration);
            }
            else
            {
                // default behavior
                allocatedEvaluator.SubmitContextAndTask(contextConfiguration, mergedTaskConfiguration);
            }
        }

        public void OnCompleted()
        {
            throw new NotImplementedException();
        }

        public void OnError(Exception error)
        {
            throw new NotImplementedException();
        }
    }
}
