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
using Org.Apache.REEF.Common.Tasks;
using Org.Apache.REEF.Driver.Context;
using Org.Apache.REEF.Driver.Evaluator;
using Org.Apache.REEF.Examples.Tasks.HelloTask;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Tang.Util;

namespace Org.Apache.REEF.Examples.AllHandlers
{
    /// <summary>
    /// A sample implementation of ActiveContext handler
    /// </summary>
    public class HelloActiveContextHandler : IObserver<IActiveContext>
    {
        [Inject]
        private HelloActiveContextHandler()
        {
        }

        /// <summary>
        /// This method prepare a Task Configuration and submit the task to the active context
        /// 
        /// </summary>
        /// <param name="activeContext"></param>
        public void OnNext(IActiveContext activeContext)
        {
            Console.WriteLine(
                string.Format(
                    CultureInfo.InvariantCulture,
                    "Active context {0} received from evaluator {1}",
                    activeContext.Id,
                    activeContext.EvaluatorId));

            IEvaluatorDescriptor evaluatorDescriptor = activeContext.EvaluatorDescriptor;
            string ipAddress = evaluatorDescriptor.NodeDescriptor.InetSocketAddress.Address.ToString();
            int port = evaluatorDescriptor.NodeDescriptor.InetSocketAddress.Port;
            string hostName = evaluatorDescriptor.NodeDescriptor.HostName;

            Console.WriteLine(
                string.Format(
                CultureInfo.InvariantCulture, 
                "The running evaluator is assigned with {0} MB of memory and is running at ip: {1} and port {2}, with hostname {3}", 
                evaluatorDescriptor.Memory, 
                ipAddress, 
                port, 
                hostName));

            IConfiguration taskConfiguration = TaskConfiguration.ConfigurationModule
                .Set(TaskConfiguration.Identifier, "bridgeCLRHelloTask_" + DateTime.Now.Ticks)
                .Set(TaskConfiguration.Task, GenericType<HelloTask>.Class)
                .Set(TaskConfiguration.OnMessage, GenericType<HelloTask.HelloDriverMessageHandler>.Class)
                .Set(TaskConfiguration.OnSendMessage, GenericType<HelloTaskMessage>.Class)
                .Build();

            activeContext.SubmitTask(taskConfiguration);
        }

        public void OnError(Exception error)
        {
            throw new NotImplementedException();
        }

        public void OnCompleted()
        {
            throw new NotImplementedException();
        }
    }
}