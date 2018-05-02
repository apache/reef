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
using System.IO;
using System.Linq;
using Org.Apache.REEF.Driver.Evaluator;
using Org.Apache.REEF.Driver.Task;
using Org.Apache.REEF.IMRU.API;
using Org.Apache.REEF.IMRU.Examples.PipelinedBroadcastReduce;
using Org.Apache.REEF.IMRU.OnREEF.Driver;
using Org.Apache.REEF.IMRU.OnREEF.Parameters;
using Org.Apache.REEF.IMRU.OnREEF.ResultHandler;
using Org.Apache.REEF.IO.PartitionedData.Random;
using Org.Apache.REEF.Network.Examples.GroupCommunication.BroadcastReduceDriverAndTasks;
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
using Org.Apache.REEF.Wake.StreamingCodec.CommonStreamingCodecs;

namespace Org.Apache.REEF.Tests.Functional.IMRU
{
    /// <summary>
    /// IMRU test base class that defines basic configurations for IMRU driver that can be shared by subclasses. 
    /// </summary>
    public abstract class IMRUBrodcastReduceTestBase : ReefFunctionalTest
    {
        protected static readonly Logger Logger = Logger.GetLogger(typeof(IMRUBrodcastReduceTestBase));
        protected const string IMRUJobName = "IMRUBroadcastReduce";

        protected const string CompletedTaskMessage = IMRUDriver<int[], int[], int[], int[]>.CompletedTaskMessage;
        protected const string RunningTaskMessage = IMRUDriver<int[], int[], int[], int[]>.RunningTaskMessage;
        protected const string FailedTaskMessage = IMRUDriver<int[], int[], int[], int[]>.FailedTaskMessage;
        protected const string FailedEvaluatorMessage = IMRUDriver<int[], int[], int[], int[]>.FailedEvaluatorMessage;
        protected const string DoneActionMessage = IMRUDriver<int[], int[], int[], int[]>.DoneActionPrefix;
        protected const string FailedActionMessage = IMRUDriver<int[], int[], int[], int[]>.FailActionPrefix;

        /// <summary>
        /// Abstract method for subclass to override it to provide configurations for driver handlers 
        /// </summary>
        protected abstract IConfiguration DriverEventHandlerConfigurations<TMapInput, TMapOutput, TResult, TPartitionType>();

        /// <summary>
        /// This method provides a default way to call TestRun. 
        /// It gets driver configurations from base class, including the DriverEventHandlerConfigurations defined by subclass,
        /// then calls TestRun for running the test.
        /// Subclass can override it if they have different parameters for the test
        /// </summary>
        protected void TestBroadCastAndReduce(bool runOnYarn,
            int numTasks,
            int chunkSize,
            int dims,
            int iterations,
            int mapperMemory,
            int updateTaskMemory,
            int numberOfRetryInRecovery = 0,
            string testFolder = DefaultRuntimeFolder,
            int? numberOfChecksBeforeCancellingJob = null)
        {
            string runPlatform = runOnYarn ? "yarn" : "local";
            TestRun(DriverConfiguration<int[], int[], int[], Stream>(
                CreateIMRUJobDefinitionBuilder(numTasks - 1, chunkSize, iterations, dims, mapperMemory, updateTaskMemory, numberOfRetryInRecovery, numberOfChecksBeforeCancellingJob),
                DriverEventHandlerConfigurations<int[], int[], int[], Stream>()),
                typeof(BroadcastReduceDriver),
                numTasks,
                "BroadcastReduceDriver",
                runPlatform,
                testFolder);
        }

        /// <summary>
        /// Build driver configuration
        /// </summary>
        protected IConfiguration DriverConfiguration<TMapInput, TMapOutput, TResult, TPartitionType>(
            IMRUJobDefinition jobDefinition,
            IConfiguration driverHandlerConfig)
        {
            string driverId = string.Format("IMRU-{0}-Driver", jobDefinition.JobName);
            IConfiguration overallPerMapConfig = null;
            var configurationSerializer = new AvroConfigurationSerializer();

            try
            {
                overallPerMapConfig = Configurations.Merge(jobDefinition.PerMapConfigGeneratorConfig.ToArray());
            }
            catch (Exception e)
            {
                Exceptions.Throw(e, "Issues in merging PerMapCOnfigGenerator configurations", Logger);
            }

            var imruDriverConfiguration = TangFactory.GetTang().NewConfigurationBuilder(new[]
            {
                driverHandlerConfig,
                CreateGroupCommunicationConfiguration<TMapInput, TMapOutput, TResult, TPartitionType>(jobDefinition.NumberOfMappers + 1,
                    driverId),
                jobDefinition.PartitionedDatasetConfiguration,
                overallPerMapConfig,
                jobDefinition.JobCancelSignalConfiguration
            })
                .BindNamedParameter(typeof(SerializedUpdateTaskStateConfiguration),
                    configurationSerializer.ToString(jobDefinition.UpdateTaskStateConfiguration))
                .BindNamedParameter(typeof(SerializedMapTaskStateConfiguration),
                    configurationSerializer.ToString(jobDefinition.MapTaskStateConfiguration))
                .BindNamedParameter(typeof(SerializedMapConfiguration),
                    configurationSerializer.ToString(jobDefinition.MapFunctionConfiguration))
                .BindNamedParameter(typeof(SerializedUpdateConfiguration),
                    configurationSerializer.ToString(jobDefinition.UpdateFunctionConfiguration))
                .BindNamedParameter(typeof(SerializedMapInputCodecConfiguration),
                    configurationSerializer.ToString(jobDefinition.MapInputCodecConfiguration))
                .BindNamedParameter(typeof(SerializedMapInputPipelineDataConverterConfiguration),
                    configurationSerializer.ToString(jobDefinition.MapInputPipelineDataConverterConfiguration))
                .BindNamedParameter(typeof(SerializedUpdateFunctionCodecsConfiguration),
                    configurationSerializer.ToString(jobDefinition.UpdateFunctionCodecsConfiguration))
                .BindNamedParameter(typeof(SerializedMapOutputPipelineDataConverterConfiguration),
                    configurationSerializer.ToString(jobDefinition.MapOutputPipelineDataConverterConfiguration))
                .BindNamedParameter(typeof(SerializedReduceConfiguration),
                    configurationSerializer.ToString(jobDefinition.ReduceFunctionConfiguration))
                .BindNamedParameter(typeof(SerializedResultHandlerConfiguration),
                    configurationSerializer.ToString(jobDefinition.ResultHandlerConfiguration))
                .BindNamedParameter(typeof(SerializedCheckpointConfiguration),
                    configurationSerializer.ToString(jobDefinition.CheckpointConfiguration))
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
            return imruDriverConfiguration;
        }

        /// <summary>
        /// Create group communication configuration
        /// </summary>
        private IConfiguration CreateGroupCommunicationConfiguration<TMapInput, TMapOutput, TResult, TPartitionType>(
            int numberOfTasks,
            string driverId)
        {
            return TangFactory.GetTang().NewConfigurationBuilder()
                .BindStringNamedParam<GroupCommConfigurationOptions.DriverId>(driverId)
                .BindStringNamedParam<GroupCommConfigurationOptions.MasterTaskId>(IMRUConstants.UpdateTaskName)
                .BindStringNamedParam<GroupCommConfigurationOptions.GroupName>(IMRUConstants.CommunicationGroupName)
                .BindIntNamedParam<GroupCommConfigurationOptions.FanOut>(IMRUConstants.TreeFanout.ToString(CultureInfo.InvariantCulture))
                .BindIntNamedParam<GroupCommConfigurationOptions.NumberOfTasks>(numberOfTasks.ToString(CultureInfo.InvariantCulture))
                .BindImplementation(GenericType<IGroupCommDriver>.Class, GenericType<GroupCommDriver>.Class)
                .Build();
        }

        /// <summary>
        /// Create IMRU Job Definition with IMRU required configurations
        /// </summary>
        protected virtual IMRUJobDefinition CreateIMRUJobDefinitionBuilder(int numberofMappers,
            int chunkSize,
            int numIterations,
            int dim,
            int mapperMemory,
            int updateTaskMemory,
            int numberOfRetryInRecovery,
            int? numberOfChecksBeforeCancellingJob = null)
        {
            var builder = new IMRUJobDefinitionBuilder()
                .SetMapFunctionConfiguration(BuildMapperFunctionConfig())
                .SetUpdateFunctionConfiguration(BuildUpdateFunctionConfiguration(numberofMappers, numIterations, dim))
                .SetMapInputCodecConfiguration(BuildMapInputCodecConfig())
                .SetUpdateFunctionCodecsConfiguration(BuildUpdateFunctionCodecsConfig())
                .SetReduceFunctionConfiguration(BuildReduceFunctionConfig())
                .SetMapInputPipelineDataConverterConfiguration(BuildDataConverterConfig(chunkSize))
                .SetMapOutputPipelineDataConverterConfiguration(BuildDataConverterConfig(chunkSize))
                .SetPartitionedDatasetConfiguration(BuildPartitionedDatasetConfiguration(numberofMappers))
                .SetResultHandlerConfiguration(BuildResultHandlerConfig())
                .SetCheckpointConfiguration(BuildCheckpointConfig())
                .SetJobName(IMRUJobName)
                .SetNumberOfMappers(numberofMappers)
                .SetMapperMemory(mapperMemory)
                .SetUpdateTaskMemory(updateTaskMemory)
                .SetMaxRetryNumberInRecovery(numberOfRetryInRecovery);

            if (numberOfChecksBeforeCancellingJob.HasValue)
            {
                var cancelConfig = TangFactory.GetTang().NewConfigurationBuilder()
                    .BindImplementation(GenericType<IJobCancelledDetector>.Class, GenericType<JobCancellationDetectoBasedOnCheckCount>.Class)
                    .BindNamedParameter(typeof(JobCancellationDetectoBasedOnCheckCount.NumberOfChecksBeforeCancelling), numberOfChecksBeforeCancellingJob.Value.ToString())
                    .BindNamedParameter(typeof(SleepIntervalParameter), "1")
                    .Build();

                builder.SetJobCancellationConfiguration(cancelConfig);
            }

            return builder.Build();
        }

        /// <summary>
        /// Build default result handler configuration. Subclass can override it.
        /// </summary>
        protected virtual IConfiguration BuildResultHandlerConfig()
        {
            return TangFactory.GetTang().NewConfigurationBuilder()
                    .BindImplementation(GenericType<IIMRUResultHandler<int[]>>.Class, GenericType<DefaultResultHandler<int[]>>.Class)
                    .Build();
        }

        /// <summary>
        /// Build checkpoint configuration. Subclass can override it.
        /// </summary>
        protected virtual IConfiguration BuildCheckpointConfig()
        {
            return TangFactory.GetTang().NewConfigurationBuilder()
                .Build();
        }

        /// <summary>
        /// Build update function configuration. Subclass can override it.
        /// </summary>
        protected virtual IConfiguration BuildUpdateFunctionConfiguration(int numberofMappers, int numIterations, int dim)
        {
            var updateFunctionConfig =
                TangFactory.GetTang().NewConfigurationBuilder(BuildUpdateFunctionConfigModule())
                    .BindNamedParameter(typeof(BroadcastReduceConfiguration.NumberOfIterations),
                        numIterations.ToString(CultureInfo.InvariantCulture))
                    .BindNamedParameter(typeof(BroadcastReduceConfiguration.Dimensions),
                        dim.ToString(CultureInfo.InvariantCulture))
                    .BindNamedParameter(typeof(BroadcastReduceConfiguration.NumWorkers),
                        numberofMappers.ToString(CultureInfo.InvariantCulture))
                    .Build();
            return updateFunctionConfig;
        }

        /// <summary>
        ///  Data Converter Configuration. Subclass can override it to have its own test Data Converter.
        /// </summary>
        protected virtual IConfiguration BuildDataConverterConfig(int chunkSize)
        {
            return TangFactory.GetTang()
                .NewConfigurationBuilder(IMRUPipelineDataConverterConfiguration<int[]>.ConfigurationModule
                    .Set(IMRUPipelineDataConverterConfiguration<int[]>.MapInputPiplelineDataConverter,
                        GenericType<PipelineIntDataConverter>.Class).Build())
                .BindNamedParameter(typeof(BroadcastReduceConfiguration.ChunkSize),
                    chunkSize.ToString(CultureInfo.InvariantCulture))
                .Build();
        }

        /// <summary>
        /// Mapper function configuration. Subclass can override it to have its own test function.
        /// </summary>
        protected virtual IConfiguration BuildMapperFunctionConfig()
        {
            return IMRUMapConfiguration<int[], int[]>.ConfigurationModule
                .Set(IMRUMapConfiguration<int[], int[]>.MapFunction,
                    GenericType<BroadcastReceiverReduceSenderMapFunction>.Class)
                .Build();
        }

        /// <summary>
        /// Set update function to IMRUUpdateConfiguration configuration module. Sub class can override it to set different function.
        /// </summary>
        protected virtual IConfiguration BuildUpdateFunctionConfigModule()
        {
            return IMRUUpdateConfiguration<int[], int[], int[]>.ConfigurationModule
                .Set(IMRUUpdateConfiguration<int[], int[], int[]>.UpdateFunction,
                    GenericType<BroadcastSenderReduceReceiverUpdateFunction>.Class)
                .Build();
        }

        /// <summary>
        /// Partition dataset configuration. Subclass can override it to have its own test dataset config
        /// </summary>
        protected virtual IConfiguration BuildPartitionedDatasetConfiguration(int numberofMappers)
        {
            return RandomInputDataConfiguration.ConfigurationModule.Set(
                RandomInputDataConfiguration.NumberOfPartitions,
                numberofMappers.ToString()).Build();
        }

        /// <summary>
        /// Map Input Codec configuration. Subclass can override it to have its own test Codec.
        /// </summary>
        protected virtual IConfiguration BuildMapInputCodecConfig()
        {
            return IMRUCodecConfiguration<int[]>.ConfigurationModule
                .Set(IMRUCodecConfiguration<int[]>.Codec, GenericType<IntArrayStreamingCodec>.Class)
                .Build();
        }

        /// <summary>
        /// Update function Codec configuration. Subclass can override it to have its own test Codec.
        /// </summary>
        protected virtual IConfiguration BuildUpdateFunctionCodecsConfig()
        {
            return IMRUCodecConfiguration<int[]>.ConfigurationModule
                .Set(IMRUCodecConfiguration<int[]>.Codec, GenericType<IntArrayStreamingCodec>.Class)
                .Build();
        }

        /// <summary>
        /// Reduce function configuration. Subclass can override it to have its own test function.
        /// </summary>
        protected virtual IConfiguration BuildReduceFunctionConfig()
        {
            return IMRUReduceFunctionConfiguration<int[]>.ConfigurationModule
                .Set(IMRUReduceFunctionConfiguration<int[]>.ReduceFunction,
                    GenericType<IntArraySumReduceFunction>.Class)
                .Build();
        }

        /// <summary>
        /// This class contains handlers for log purpose only
        /// </summary>
        protected sealed class MessageLogger :
            IObserver<ICompletedTask>,
            IObserver<IFailedEvaluator>,
            IObserver<IFailedTask>,
            IObserver<IRunningTask>
        {
            [Inject]
            private MessageLogger()
            {
            }

            public void OnCompleted()
            {
                throw new NotImplementedException();
            }

            public void OnError(Exception error)
            {
                throw new NotImplementedException();
            }

            public void OnNext(ICompletedTask value)
            {
                Logger.Log(Level.Info, CompletedTaskMessage + " " + value.Id + " " + value.ActiveContext.EvaluatorId);
            }

            public void OnNext(IFailedTask value)
            {
                Logger.Log(Level.Info, FailedTaskMessage + " " + value.Id + " " + value.GetActiveContext().Value.EvaluatorId);
            }

            public void OnNext(IFailedEvaluator value)
            {
                Logger.Log(Level.Info, FailedEvaluatorMessage + " " + value.Id + " " + (value.FailedTask.IsPresent() ? value.FailedTask.Value.Id : "no task"));
            }

            public void OnNext(IRunningTask value)
            {
                Logger.Log(Level.Info, RunningTaskMessage + " " + value.Id + " " + value.ActiveContext.EvaluatorId);
            }
        }

        private sealed class JobCancellationDetectoBasedOnCheckCount : IJobCancelledDetector
        {
            private int _checkCount = 0;
            private int _numberOfChecksBeforeCancelling;

            [Inject]
            JobCancellationDetectoBasedOnCheckCount([Parameter(typeof(NumberOfChecksBeforeCancelling))] int numberOfChecksBeforeCancelling)
            {
                _numberOfChecksBeforeCancelling = numberOfChecksBeforeCancelling;
            }

            public bool IsJobCancelled(out string cancellationMessage)
            {
                _checkCount++;
                cancellationMessage = "IsCancelled check count: " + _checkCount;
                return _checkCount > _numberOfChecksBeforeCancelling;
            }

            [NamedParameter("Number of IsCancelled checks before cancelling job", "numberOfChecksBeforeCancelling", "0")]
            internal sealed class NumberOfChecksBeforeCancelling : Name<int>
            {
            }
        }
    }
}