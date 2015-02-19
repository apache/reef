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
using System.Collections.Generic;
using System.Linq;
using Org.Apache.REEF.Common.Io;
using Org.Apache.REEF.Common.Tasks;
using Org.Apache.REEF.Driver;
using Org.Apache.REEF.Driver.Bridge;
using Org.Apache.REEF.Driver.Context;
using Org.Apache.REEF.Driver.Evaluator;
using Org.Apache.REEF.Network.Group.Driver;
using Org.Apache.REEF.Network.Group.Driver.Impl;
using Org.Apache.REEF.Network.Group.Operators;
using Org.Apache.REEF.Network.Group.Operators.Impl;
using Org.Apache.REEF.Network.NetworkService;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Formats;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Tang.Util;
using Org.Apache.REEF.Utilities.Logging;
using Org.Apache.REEF.Wake.Remote.Impl;

namespace Org.Apache.REEF.Tests.Functional.MPI.ScatterReduceTest
{
    public class ScatterReduceDriver : IStartHandler, IObserver<IEvaluatorRequestor>, IObserver<IAllocatedEvaluator>, IObserver<IActiveContext>, IObserver<IFailedEvaluator>
    {
        private static readonly Logger LOGGER = Logger.GetLogger(typeof(ScatterReduceDriver));

        private readonly int _numEvaluators;

        private readonly IMpiDriver _mpiDriver;
        private readonly ICommunicationGroupDriver _commGroup;
        private readonly TaskStarter _mpiTaskStarter;

        [Inject]
        public ScatterReduceDriver(
            [Parameter(typeof(MpiTestConfig.NumEvaluators))] int numEvaluators,
            [Parameter(typeof(MpiTestConfig.FanOut))] int fanOut,
            AvroConfigurationSerializer confSerializer)
        {
            Identifier = "BroadcastStartHandler";
            _numEvaluators = numEvaluators;

            _mpiDriver = new MpiDriver(
                MpiTestConstants.DriverId,
                MpiTestConstants.MasterTaskId,
                fanOut,
                confSerializer);

            _commGroup = _mpiDriver.NewCommunicationGroup(
                MpiTestConstants.GroupName, 
                numEvaluators)
                    .AddScatter(
                        MpiTestConstants.ScatterOperatorName,
                        new ScatterOperatorSpec<int>(
                            MpiTestConstants.MasterTaskId,
                            new IntCodec()))
                    .AddReduce(
                        MpiTestConstants.ReduceOperatorName,
                        new ReduceOperatorSpec<int>(
                            MpiTestConstants.MasterTaskId,
                            new IntCodec(), 
                            new SumFunction()))
                    .Build();

            _mpiTaskStarter = new TaskStarter(_mpiDriver, numEvaluators);

            CreateClassHierarchy();
        }

        public string Identifier { get; set; }

        public void OnNext(IEvaluatorRequestor evaluatorRequestor)
        {
            EvaluatorRequest request = new EvaluatorRequest(_numEvaluators, 512, 2, "WonderlandRack", "BroadcastEvaluator");
            evaluatorRequestor.Submit(request);
        }

        public void OnNext(IAllocatedEvaluator allocatedEvaluator)
        {
            IConfiguration contextConf = _mpiDriver.GetContextConfiguration();
            IConfiguration serviceConf = _mpiDriver.GetServiceConfiguration();
            allocatedEvaluator.SubmitContextAndService(contextConf, serviceConf);
        }

        public void OnNext(IActiveContext activeContext)
        {
            if (_mpiDriver.IsMasterTaskContext(activeContext))
            {
                // Configure Master Task
                IConfiguration partialTaskConf = TaskConfiguration.ConfigurationModule
                    .Set(TaskConfiguration.Identifier, MpiTestConstants.MasterTaskId)
                    .Set(TaskConfiguration.Task, GenericType<MasterTask>.Class)
                    .Build();

                _commGroup.AddTask(MpiTestConstants.MasterTaskId);
                _mpiTaskStarter.QueueTask(partialTaskConf, activeContext);
            }
            else
            {
                // Configure Slave Task
                string slaveTaskId = MpiTestConstants.SlaveTaskId +
                    _mpiDriver.GetContextNum(activeContext);

                IConfiguration partialTaskConf = TaskConfiguration.ConfigurationModule
                    .Set(TaskConfiguration.Identifier, slaveTaskId)
                    .Set(TaskConfiguration.Task, GenericType<SlaveTask>.Class)
                    .Build();

                _commGroup.AddTask(slaveTaskId);
                _mpiTaskStarter.QueueTask(partialTaskConf, activeContext);
            }
        }

        public void OnNext(IFailedEvaluator value)
        {
        }

        public void OnError(Exception error)
        {
        }

        public void OnCompleted()
        {
        }

        private void CreateClassHierarchy()
        {
            HashSet<string> clrDlls = new HashSet<string>();
            clrDlls.Add(typeof(IDriver).Assembly.GetName().Name);
            clrDlls.Add(typeof(ITask).Assembly.GetName().Name);
            clrDlls.Add(typeof(ScatterReduceDriver).Assembly.GetName().Name);
            clrDlls.Add(typeof(INameClient).Assembly.GetName().Name);
            clrDlls.Add(typeof(INetworkService<>).Assembly.GetName().Name);

            ClrHandlerHelper.GenerateClassHierarchy(clrDlls);
        }

        private class SumFunction : IReduceFunction<int>
        {
            [Inject]
            public SumFunction()
            {
            }

            public int Reduce(IEnumerable<int> elements)
            {
                return elements.Sum();
            }
        }
    }
}
