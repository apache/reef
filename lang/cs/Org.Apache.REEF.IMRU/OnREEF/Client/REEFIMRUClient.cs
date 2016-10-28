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
using System.Diagnostics;
using System.Globalization;
using System.Linq;
using Org.Apache.REEF.Client.API;
using Org.Apache.REEF.Client.Common;
using Org.Apache.REEF.Driver;
using Org.Apache.REEF.IMRU.API;
using Org.Apache.REEF.IMRU.OnREEF.Driver;
using Org.Apache.REEF.IMRU.OnREEF.Parameters;
using Org.Apache.REEF.Network.Group.Config;
using Org.Apache.REEF.Network.Group.Driver;
using Org.Apache.REEF.Network.Group.Driver.Impl;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Formats;
using Org.Apache.REEF.Tang.Implementations.Configuration;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Tang.Util;
using Org.Apache.REEF.Utilities.Diagnostics;
using Org.Apache.REEF.Utilities.Logging;
using TraceLevel = System.Diagnostics.TraceLevel;

namespace Org.Apache.REEF.IMRU.OnREEF.Client
{
    /// <summary>
    /// Implements the IMRU client API on REEF.
    /// </summary>
    internal sealed class REEFIMRUClient : IIMRUClient
    {
        private static readonly Logger Logger = Logger.GetLogger(typeof(REEFIMRUClient));

        private readonly IREEFClient _reefClient;
        private readonly JobRequestBuilder _jobRequestBuilder;
        private readonly AvroConfigurationSerializer _configurationSerializer;
        private IJobSubmissionResult _jobSubmissionResult;

        [Inject]
        private REEFIMRUClient(
            IREEFClient reefClient, 
            AvroConfigurationSerializer configurationSerializer,
            JobRequestBuilder jobRequestBuilder)
        {
            _reefClient = reefClient;
            _configurationSerializer = configurationSerializer;
            _jobRequestBuilder = jobRequestBuilder;
        }

        /// <summary>
        /// Submits the job to reefClient
        /// </summary>
        /// <typeparam name="TMapInput">The type of the side information provided to the Map function</typeparam>
        /// <typeparam name="TMapOutput">The return type of the Map function</typeparam>
        /// <typeparam name="TResult">The return type of the computation.</typeparam>
        /// <typeparam name="TPartitionType">Type of data partition (Generic type in IInputPartition)</typeparam>
        /// <param name="jobDefinition">IMRU job definition given by the user</param>
        /// <returns>Null as results will be later written to some directory</returns>
        IEnumerable<TResult> IIMRUClient.Submit<TMapInput, TMapOutput, TResult, TPartitionType>(IMRUJobDefinition jobDefinition)
        {
            string driverId = string.Format("IMRU-{0}-Driver", jobDefinition.JobName);
            IConfiguration overallPerMapConfig = null;

            try
            {
                overallPerMapConfig = Configurations.Merge(jobDefinition.PerMapConfigGeneratorConfig.ToArray());
            }
            catch (Exception e)
            {
                Exceptions.Throw(e, "Issues in merging PerMapCOnfigGenerator configurations", Logger);
            }

            // The driver configuration contains all the needed bindings.
            var imruDriverConfiguration = TangFactory.GetTang().NewConfigurationBuilder(new[]
            {
                DriverConfiguration.ConfigurationModule
                    .Set(DriverConfiguration.OnEvaluatorAllocated,
                        GenericType<IMRUDriver<TMapInput, TMapOutput, TResult, TPartitionType>>.Class)
                    .Set(DriverConfiguration.OnDriverStarted,
                        GenericType<IMRUDriver<TMapInput, TMapOutput, TResult, TPartitionType>>.Class)
                    .Set(DriverConfiguration.OnContextActive,
                        GenericType<IMRUDriver<TMapInput, TMapOutput, TResult, TPartitionType>>.Class)
                    .Set(DriverConfiguration.OnTaskCompleted,
                        GenericType<IMRUDriver<TMapInput, TMapOutput, TResult, TPartitionType>>.Class)
                    .Set(DriverConfiguration.OnEvaluatorFailed,
                        GenericType<IMRUDriver<TMapInput, TMapOutput, TResult, TPartitionType>>.Class)
                    .Set(DriverConfiguration.OnContextFailed,
                        GenericType<IMRUDriver<TMapInput, TMapOutput, TResult, TPartitionType>>.Class)
                    .Set(DriverConfiguration.OnTaskFailed,
                        GenericType<IMRUDriver<TMapInput, TMapOutput, TResult, TPartitionType>>.Class)
                    .Set(DriverConfiguration.OnTaskRunning,
                        GenericType<IMRUDriver<TMapInput, TMapOutput, TResult, TPartitionType>>.Class)
                    .Set(DriverConfiguration.CustomTraceLevel, TraceLevel.Info.ToString())
                    .Set(DriverConfiguration.OnDriverStarted, GenericType<JobLifeCycleManager>.Class)
                    .Build(),
                TangFactory.GetTang().NewConfigurationBuilder()
                    .BindStringNamedParam<GroupCommConfigurationOptions.DriverId>(driverId)
                    .BindStringNamedParam<GroupCommConfigurationOptions.MasterTaskId>(IMRUConstants.UpdateTaskName)
                    .BindStringNamedParam<GroupCommConfigurationOptions.GroupName>(IMRUConstants.CommunicationGroupName)
                    .BindIntNamedParam<GroupCommConfigurationOptions.FanOut>(
                        IMRUConstants.TreeFanout.ToString(CultureInfo.InvariantCulture)
                            .ToString(CultureInfo.InvariantCulture))
                    .BindIntNamedParam<GroupCommConfigurationOptions.NumberOfTasks>(
                        (jobDefinition.NumberOfMappers + 1).ToString(CultureInfo.InvariantCulture))
                    .BindImplementation(GenericType<IGroupCommDriver>.Class, GenericType<GroupCommDriver>.Class)
                    .Build(),
                jobDefinition.PartitionedDatasetConfiguration,
                overallPerMapConfig,
                jobDefinition.JobCancelSignalConfiguration
            })
                .BindNamedParameter(typeof(SerializedUpdateTaskStateConfiguration),
                    _configurationSerializer.ToString(jobDefinition.UpdateTaskStateConfiguration))
                .BindNamedParameter(typeof(SerializedMapTaskStateConfiguration),
                    _configurationSerializer.ToString(jobDefinition.MapTaskStateConfiguration))
                .BindNamedParameter(typeof(SerializedMapConfiguration),
                    _configurationSerializer.ToString(jobDefinition.MapFunctionConfiguration))
                .BindNamedParameter(typeof(SerializedUpdateConfiguration),
                    _configurationSerializer.ToString(jobDefinition.UpdateFunctionConfiguration))
                .BindNamedParameter(typeof(SerializedMapInputCodecConfiguration),
                    _configurationSerializer.ToString(jobDefinition.MapInputCodecConfiguration))
                .BindNamedParameter(typeof(SerializedMapInputPipelineDataConverterConfiguration),
                    _configurationSerializer.ToString(jobDefinition.MapInputPipelineDataConverterConfiguration))
                .BindNamedParameter(typeof(SerializedUpdateFunctionCodecsConfiguration),
                    _configurationSerializer.ToString(jobDefinition.UpdateFunctionCodecsConfiguration))
                .BindNamedParameter(typeof(SerializedMapOutputPipelineDataConverterConfiguration),
                    _configurationSerializer.ToString(jobDefinition.MapOutputPipelineDataConverterConfiguration))
                .BindNamedParameter(typeof(SerializedReduceConfiguration),
                    _configurationSerializer.ToString(jobDefinition.ReduceFunctionConfiguration))
                .BindNamedParameter(typeof(SerializedResultHandlerConfiguration),
                    _configurationSerializer.ToString(jobDefinition.ResultHandlerConfiguration))
                .BindNamedParameter(typeof(MemoryPerMapper),
                    jobDefinition.MapperMemory.ToString(CultureInfo.InvariantCulture))
                .BindNamedParameter(typeof(MemoryForUpdateTask),
                    jobDefinition.UpdateTaskMemory.ToString(CultureInfo.InvariantCulture))
                .BindNamedParameter(typeof(CoresPerMapper),
                    jobDefinition.MapTaskCores.ToString(CultureInfo.InvariantCulture))
                .BindNamedParameter(typeof(CoresForUpdateTask),
                    jobDefinition.UpdateTaskCores.ToString(CultureInfo.InvariantCulture))
                .BindNamedParameter(typeof(MaxRetryNumberInRecovery),
                    jobDefinition.MaxRetryNumberInRecovery.ToString(CultureInfo.InvariantCulture))
                .BindNamedParameter(typeof(InvokeGC),
                    jobDefinition.InvokeGarbageCollectorAfterIteration.ToString(CultureInfo.InvariantCulture))
                .Build();

            // The JobSubmission contains the Driver configuration as well as the files needed on the Driver.
            var imruJobSubmission = _jobRequestBuilder
                .AddDriverConfiguration(imruDriverConfiguration)
                .AddGlobalAssemblyForType(typeof(IMRUDriver<TMapInput, TMapOutput, TResult, TPartitionType>))
                .SetJobIdentifier(jobDefinition.JobName)
                .SetDriverMemory(5000)
                .Build();

            _jobSubmissionResult = _reefClient.SubmitAndGetJobStatus(imruJobSubmission);

            return null;
        }

        /// <summary>
        /// DriverHttpEndPoint returned by IReefClient after job submission
        /// </summary>
        public IJobSubmissionResult JobSubmissionResult
        {
            get { return _jobSubmissionResult; }
        }
    }
}