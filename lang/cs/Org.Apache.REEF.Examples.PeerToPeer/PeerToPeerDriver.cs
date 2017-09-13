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
using Org.Apache.REEF.Driver;
using Org.Apache.REEF.Driver.Evaluator;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Tang.Util;
using Org.Apache.REEF.Utilities.Logging;
using Org.Apache.REEF.Driver.Context;
using Org.Apache.REEF.Common.Context;
using Org.Apache.REEF.Common.Services;
using Org.Apache.REEF.Network.NetworkService;
using Org.Apache.REEF.Wake.Remote;
using Org.Apache.REEF.Common.Io;
using Org.Apache.REEF.Network.Naming;
using System.Net;
using Org.Apache.REEF.Wake.StreamingCodec;
using Org.Apache.REEF.Wake.StreamingCodec.CommonStreamingCodecs;
using Org.Apache.REEF.Examples.PeerToPeer.Communication;

namespace Org.Apache.REEF.Examples.PeerToPeer
{
    /// <summary>
    /// The Driver for PeerToPeer: It requests two Evaluators and then submits the Task to them.
    /// </summary>
    public sealed class PeerToPeerDriver : 
        IObserver<IAllocatedEvaluator>, 
        IObserver<IDriverStarted>, 
        IObserver<IActiveContext>
    {
        private static readonly Logger Logger = Logger.GetLogger(typeof(PeerToPeerDriver));
        private readonly IEvaluatorRequestor _evaluatorRequestor;
        private static readonly int _TotalTasks = 2; // Hard-code the number of tasks
        private static int _tasksLaunched;
        private readonly object _lockObj = new object();
        private readonly string _nameServerAddr;
        private readonly int _nameServerPort;

        [Inject]
        private PeerToPeerDriver(IEvaluatorRequestor evaluatorRequestor, INameServer nameServer)
        {
            _evaluatorRequestor = evaluatorRequestor;
            _tasksLaunched = 0;
            IPEndPoint localEndpoint = nameServer.LocalEndpoint;
            _nameServerAddr = localEndpoint.Address.ToString();
            _nameServerPort = localEndpoint.Port;
        }

        /// <summary>
        /// Called to start the user mode driver
        /// </summary>
        /// <param name="driverStarted"></param>
        public void OnNext(IDriverStarted driverStarted)
        {
            Logger.Log(Level.Info, string.Format("TwoFriendsDriver started at {0}", driverStarted.StartTime));

            // Formulate the request
            int evaluators = 2;
            int memory = 512;
            int core = 1;
            var request = _evaluatorRequestor.NewBuilder()
                .SetNumber(evaluators)
                .SetMegabytes(memory)
                .SetCores(core)
                .Build();

            _evaluatorRequestor.Submit(request);
        }

        /// <summary>
        /// Submits the FriendTask to the Evaluator.
        /// </summary>
        /// <param name="allocatedEvaluator"></param>
        public void OnNext(IAllocatedEvaluator allocatedEvaluator)
        {
            // Define the context for the evaluator
            var contextConfiguration = ContextConfiguration.ConfigurationModule
                    .Set(ContextConfiguration.Identifier, "Context_" + Guid.NewGuid().ToString("N"))
                    .Build();

            var serviceConfig = TangFactory.GetTang().NewConfigurationBuilder(
                    ServiceConfiguration.ConfigurationModule
                        .Set(ServiceConfiguration.Services, GenericType<StreamingNetworkService<Message<string>>>.Class)
                        .Build())
                .BindImplementation(
                    GenericType<IStreamingCodec<string>>.Class,
                    GenericType<StringStreamingCodec>.Class)
                .BindImplementation(GenericType<IStreamingCodec<Message<string>>>.Class,
                GenericType<MessageStreamingCodec<string>>.Class)
                .BindImplementation(
                    GenericType<IObserver<NsMessage<Message<string>>>>.Class,
                    GenericType<MessageObserver<string>>.Class)
                .BindImplementation(
                    GenericType<IObserver<IRemoteMessage<NsMessage<Message<string>>>>>.Class,
                    GenericType<MessageObserver<string>>.Class)
                .BindImplementation(GenericType<INameClient>.Class,
                    GenericType<NameClient>.Class)
                .BindNamedParameter<NamingConfigurationOptions.NameServerAddress, string>(
                    GenericType<NamingConfigurationOptions.NameServerAddress>.Class,
                    _nameServerAddr)
                .BindNamedParameter<NamingConfigurationOptions.NameServerPort, int>(
                    GenericType<NamingConfigurationOptions.NameServerPort>.Class,
                    _nameServerPort.ToString(CultureInfo.InvariantCulture))
                .BindNamedParameter<PeerConfiguration.NumberOfPeers, int>(
                    GenericType<PeerConfiguration.NumberOfPeers>.Class, _TotalTasks.ToString(CultureInfo.InvariantCulture))
                .Build();

            allocatedEvaluator.SubmitContextAndService(contextConfiguration, serviceConfig);
        }

        public void OnNext(IActiveContext activeContext)
        {
            // Define the task for the evaluator
            string id;
            lock (_lockObj)
            {
                id = string.Format(Constants.TaskIdFormat, _tasksLaunched++);
            }

            var taskConfiguration = TaskConfiguration.ConfigurationModule
                .Set(TaskConfiguration.Identifier, id)
                .Set(TaskConfiguration.Task, GenericType<PeerTask>.Class)
                .Build();

            activeContext.SubmitTask(taskConfiguration);
        }

        public void OnError(Exception error)
        {
            throw error;
        }

        public void OnCompleted()
        {
        }
    }
}