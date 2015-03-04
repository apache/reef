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
using System.Globalization;
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
using Org.Apache.REEF.Network.Group.Pipelining;
using Org.Apache.REEF.Network.Group.Operators.Impl;
using Org.Apache.REEF.Network.NetworkService;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Formats;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Tang.Util;
using Org.Apache.REEF.Utilities.Logging;
using Org.Apache.REEF.Wake.Remote;
using Org.Apache.REEF.Wake.Remote.Impl;
using Org.Apache.REEF.Network.Group.Topology;
using Org.Apache.REEF.Network.Group.CommonOperators.Driver;
using Org.Apache.REEF.Network.Group.CommonOperators;

namespace Org.Apache.REEF.Tests.Functional.MPI.CommonOperatorsTest
{
    public class CommonOperatorsDriver : IStartHandler, IObserver<IEvaluatorRequestor>, IObserver<IAllocatedEvaluator>, IObserver<IActiveContext>, IObserver<IFailedEvaluator>
    {
        private static readonly Logger LOGGER = Logger.GetLogger(typeof(CommonOperatorsDriver));

        private readonly int _numEvaluators;
        private readonly int _numIterations;
        
        private readonly IMpiDriver _mpiDriver;
        private readonly ICommunicationGroupDriver _commGroup;
        private readonly TaskStarter _mpiTaskStarter;

        [Inject]
        public CommonOperatorsDriver(
            [Parameter(typeof(MpiTestConfig.NumEvaluators))] int numEvaluators,
            [Parameter(typeof(MpiTestConfig.NumIterations))] int numIterations,
            AvroConfigurationSerializer confSerializer)
        {
            Identifier = "BroadcastStartHandler";
            _numEvaluators = numEvaluators;
            _numIterations = numIterations;

            _mpiDriver = new MpiDriver(
               MpiTestConstants.DriverId,
               MpiTestConstants.MasterTaskId,
               MpiTestConstants.FanOut,
               confSerializer);

            _commGroup = _mpiDriver.NewCommunicationGroup(
                MpiTestConstants.GroupName,
                numEvaluators);

            switch (CommonOperatorsTestConstants.OperatorName)
            {
                case DefaultCommonOperatorNames.DoubleArrayBroadcastName:
                    AddCommonBroadcastOperators.AddDoubleArrayBroadcastOperator(_commGroup, MpiTestConstants.MasterTaskId);
                    break;

                case DefaultCommonOperatorNames.IntArrayBroadcastName:
                    AddCommonBroadcastOperators.AddIntegerArrayBroadcastOperator(_commGroup, MpiTestConstants.MasterTaskId);
                    break;

                case DefaultCommonOperatorNames.FloatArrayBroadcastName:
                    AddCommonBroadcastOperators.AddFloatArrayBroadcastOperator(_commGroup, MpiTestConstants.MasterTaskId);
                    break;

                case DefaultCommonOperatorNames.DoubleBroadcastName:
                    AddCommonBroadcastOperators.AddDoubleBroadcastOperator(_commGroup, MpiTestConstants.MasterTaskId);
                    break;

                case DefaultCommonOperatorNames.IntBroadcastName:
                    AddCommonBroadcastOperators.AddIntegerBroadcastOperator(_commGroup, MpiTestConstants.MasterTaskId);
                    break;
                
                case DefaultCommonOperatorNames.FloatBroadcastName:
                    AddCommonBroadcastOperators.AddFloatBroadcastOperator(_commGroup, MpiTestConstants.MasterTaskId);
                    break;

                case DefaultCommonOperatorNames.DoubleArrayMergeReduceName:
                    AddCommonReduceOperators.AddDoubleArrayMergeReduceOperator(_commGroup, MpiTestConstants.MasterTaskId);
                    break;

                case DefaultCommonOperatorNames.DoubleArraySumReduceName:
                    AddCommonReduceOperators.AddDoubleArraySumReduceOperator(_commGroup, MpiTestConstants.MasterTaskId);
                    break;

                case DefaultCommonOperatorNames.FloatArrayMergeReduceName:
                    AddCommonReduceOperators.AddFloatArrayMergeReduceOperator(_commGroup, MpiTestConstants.MasterTaskId);
                    break;
               
                case DefaultCommonOperatorNames.FloatArraySumReduceName:
                    AddCommonReduceOperators.AddFloatArraySumReduceOperator(_commGroup, MpiTestConstants.MasterTaskId);
                    break;

                case DefaultCommonOperatorNames.IntArrayMergeReduceName:
                    AddCommonReduceOperators.AddIntegerArrayMergeReduceOperator(_commGroup, MpiTestConstants.MasterTaskId);
                    break;

                case DefaultCommonOperatorNames.IntArraySumReduceName:
                    AddCommonReduceOperators.AddIntegerArraySumReduceOperator(_commGroup, MpiTestConstants.MasterTaskId);
                    break;

                case DefaultCommonOperatorNames.DoubleMaxReduceName:
                    AddCommonReduceOperators.AddDoubleMaxReduceOperator(_commGroup, MpiTestConstants.MasterTaskId);
                    break;

                case DefaultCommonOperatorNames.DoubleMinReduceName:
                    AddCommonReduceOperators.AddDoubleMinReduceOperator(_commGroup, MpiTestConstants.MasterTaskId);
                    break;

                case DefaultCommonOperatorNames.DoubleSumReduceName:
                    AddCommonReduceOperators.AddDoubleSumReduceOperator(_commGroup, MpiTestConstants.MasterTaskId);
                    break;

                case DefaultCommonOperatorNames.FloatMaxReduceName:
                    AddCommonReduceOperators.AddFloatMaxReduceOperator(_commGroup, MpiTestConstants.MasterTaskId);
                    break;

                case DefaultCommonOperatorNames.FloatMinReduceName:
                    AddCommonReduceOperators.AddFloatMinReduceOperator(_commGroup, MpiTestConstants.MasterTaskId);
                    break;

                case DefaultCommonOperatorNames.FloatSumReduceName:
                    LOGGER.Log(Level.Info, "********************88Inside deiver n FloatSumReduce operator****************");
                    AddCommonReduceOperators.AddFloatSumReduceOperator(_commGroup, MpiTestConstants.MasterTaskId);
                    break;

                case DefaultCommonOperatorNames.IntMaxReduceName:
                    AddCommonReduceOperators.AddIntegerMaxReduceOperator(_commGroup, MpiTestConstants.MasterTaskId);
                    break;

                case DefaultCommonOperatorNames.IntMinReduceName:
                    AddCommonReduceOperators.AddIntegerMinReduceOperator(_commGroup, MpiTestConstants.MasterTaskId);
                    break;

                case DefaultCommonOperatorNames.IntSumReduceName:
                    AddCommonReduceOperators.AddIntegerSumReduceOperator(_commGroup, MpiTestConstants.MasterTaskId);
                    break;
            }
            
            _commGroup.Build();
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
                LOGGER.Log(Level.Info, "******* Master ID " + activeContext.Id );

                // Configure Master Task
                IConfiguration partialTaskConf = TangFactory.GetTang().NewConfigurationBuilder(
                    TaskConfiguration.ConfigurationModule
                        .Set(TaskConfiguration.Identifier, MpiTestConstants.MasterTaskId)
                        .Set(TaskConfiguration.Task, GenericType<CommonOperatorsMasterTask>.Class)
                        .Build())
                    .BindNamedParameter<MpiTestConfig.NumEvaluators, int>(
                        GenericType<MpiTestConfig.NumEvaluators>.Class,
                        _numEvaluators.ToString(CultureInfo.InvariantCulture))
                    .Build();

                _commGroup.AddTask(MpiTestConstants.MasterTaskId);
                _mpiTaskStarter.QueueTask(partialTaskConf, activeContext);
            }
            else
            {
                // Configure Slave Task
                string slaveTaskId = "SlaveTask-" + activeContext.Id;
                IConfiguration partialTaskConf = TangFactory.GetTang().NewConfigurationBuilder(
                    TaskConfiguration.ConfigurationModule
                        .Set(TaskConfiguration.Identifier, slaveTaskId)
                        .Set(TaskConfiguration.Task, GenericType<CommonOperatorsSlaveTask>.Class)
                        .Build())
                    .BindNamedParameter<MpiTestConfig.NumEvaluators, int>(
                        GenericType<MpiTestConfig.NumEvaluators>.Class,
                        _numEvaluators.ToString(CultureInfo.InvariantCulture))
                    .BindNamedParameter<MpiTestConfig.EvaluatorId, string>(
                        GenericType<MpiTestConfig.EvaluatorId>.Class,
                        activeContext.Id)
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
            clrDlls.Add(typeof(CommonOperatorsDriver).Assembly.GetName().Name);
            clrDlls.Add(typeof(INameClient).Assembly.GetName().Name);
            clrDlls.Add(typeof(INetworkService<>).Assembly.GetName().Name);

            ClrHandlerHelper.GenerateClassHierarchy(clrDlls);
        }        
    }
}