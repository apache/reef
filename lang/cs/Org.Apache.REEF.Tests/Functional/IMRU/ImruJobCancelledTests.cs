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
using System.Collections;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Reflection;
using Org.Apache.REEF.Common.Catalog;
using Org.Apache.REEF.Driver;
using Org.Apache.REEF.Driver.Bridge;
using Org.Apache.REEF.Driver.Bridge.Events;
using Org.Apache.REEF.Driver.Evaluator;
using Org.Apache.REEF.IMRU.API;
using Org.Apache.REEF.IMRU.OnREEF.Driver;
using Org.Apache.REEF.IMRU.OnREEF.Driver.StateMachine;
using Org.Apache.REEF.IMRU.OnREEF.Parameters;
using Org.Apache.REEF.IO.PartitionedData;
using Org.Apache.REEF.Network.Group.Config;
using Org.Apache.REEF.Network.Group.Driver;
using Org.Apache.REEF.Network.Group.Driver.Impl;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Formats;
using Org.Apache.REEF.Tang.Implementations.Configuration;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Tang.Util;
using Xunit;

namespace Org.Apache.REEF.Tests.Functional.IMRU
{
    public class ImruJobCancelledTests
    {
        /// <summary>
        /// Verifies that IMRU driver handles cancel signal: 
        /// changes state to Fail and throw exception with "cancelled" message
        /// </summary>
        [Fact]
        public void ImruDriver_HandlesCancelledEventAfterStart()
        {
            var driver = TangFactory
                    .GetTang()
                    .NewInjector(GetDriverConfig<ImruDriverTestType, ImruDriverTestType, ImruDriverTestType, ImruDriverTestType>())
                    .GetInstance(typeof(IMRUDriver<ImruDriverTestType, ImruDriverTestType, ImruDriverTestType, ImruDriverTestType>))
                as IMRUDriver<ImruDriverTestType, ImruDriverTestType, ImruDriverTestType, ImruDriverTestType>;

            IDriverStarted startedEvent = null;
            driver.OnNext(startedEvent);

            var cancelMessage = "cancel_" + Guid.NewGuid();
            var cancelTime = DateTime.Now;
            IJobCancelled cancelledEvent = new JobCancelled(cancelTime, cancelMessage);

            Assert.False(GetDriverState(driver).CurrentState == SystemState.Fail, "driver's state is Fail after Onstarted event");

            AssertExceptionThrown<ApplicationException>(
                () => driver.OnNext(cancelledEvent),
                expectedExceptionMessageContent: new[] { "Job cancelled", cancelTime.ToString("u"), cancelMessage },
                assertMessagePrefix: "Cancel event handler failed to throw expected exception");

            var stateAfterCancel = GetDriverState(driver);
            Assert.True(stateAfterCancel.CurrentState == SystemState.Fail, "invalid driver state after cancel event: expected= Fail, actual=" + stateAfterCancel.CurrentState);
        }

        private SystemStateMachine GetDriverState(object driver)
        {
            return driver.GetType()
                .GetField("_systemState", BindingFlags.Instance | BindingFlags.NonPublic)
                .GetValue(driver) as SystemStateMachine;
        }

        private void AssertExceptionThrown<TException>(Action ationWithException,
            IEnumerable<string> expectedExceptionMessageContent,
            string assertMessagePrefix)
        {
            try
            {
                ationWithException();
                Assert.True(false, assertMessagePrefix + " action did not result in any exception");
            }
            catch (Exception ex)
            {
                Assert.True(ex is TException,
                    string.Format("{0}:  expected exception of type: {1}", assertMessagePrefix, typeof(TException)));

                var missingContent = expectedExceptionMessageContent
                    .Where(expectedContent => !ex.Message.Contains(expectedContent));

                Assert.False(
                    missingContent.Any(),
                    string.Format("{0}: Did not find missing content in exception message. Missing content: {1}, actual message: {2}", assertMessagePrefix, string.Join(" | ", missingContent), ex.Message));
            }
        }

        private IConfiguration GetDriverConfig<TMapInput, TMapOutput, TResult, TPartitionType>()
        {
            var testConfig = TangFactory.GetTang().NewConfigurationBuilder()
                .BindImplementation(GenericType<IPartitionedInputDataSet>.Class, GenericType<ImruDriverTestType>.Class)
                .BindImplementation(GenericType<IEvaluatorRequestor>.Class, GenericType<ImruDriverTestType>.Class)
                .Build();

            var jobDefinition = new IMRUJobDefinitionBuilder()
                .SetJobName("Test")
                .SetMapFunctionConfiguration(testConfig)
                .SetMapInputCodecConfiguration(testConfig)
                .SetUpdateFunctionCodecsConfiguration(testConfig)
                .SetReduceFunctionConfiguration(testConfig)
                .SetUpdateFunctionConfiguration(testConfig)
                .SetPartitionedDatasetConfiguration(testConfig)
                .Build();

            var _configurationSerializer = new AvroConfigurationSerializer();

            var overallPerMapConfig = Configurations.Merge(jobDefinition.PerMapConfigGeneratorConfig.ToArray());
            var driverConfig = TangFactory.GetTang().NewConfigurationBuilder(new[]
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
                        .BindStringNamedParam<GroupCommConfigurationOptions.DriverId>("driverId")
                        .BindStringNamedParam<GroupCommConfigurationOptions.MasterTaskId>(IMRUConstants.UpdateTaskName)
                        .BindStringNamedParam<GroupCommConfigurationOptions.GroupName>(
                            IMRUConstants.CommunicationGroupName)
                        .BindIntNamedParam<GroupCommConfigurationOptions.FanOut>(
                            IMRUConstants.TreeFanout.ToString(CultureInfo.InvariantCulture)
                                .ToString(CultureInfo.InvariantCulture))
                        .BindIntNamedParam<GroupCommConfigurationOptions.NumberOfTasks>(
                            (jobDefinition.NumberOfMappers + 1).ToString(CultureInfo.InvariantCulture))
                        .BindImplementation(GenericType<IGroupCommDriver>.Class, GenericType<GroupCommDriver>.Class)
                        .Build(),
                    jobDefinition.PartitionedDatasetConfiguration,
                    overallPerMapConfig
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
                .AddOptionalConfiguration(jobDefinition.JobCancelSignalConfiguration)
                .Build();
                
            return driverConfig;
        }

        /// <summary>
        /// Simple Type to help with Tang injection when constructing IMRUDriver.
        /// Cares minimum implmentation to satisfy new driver instance for test scenarios
        /// </summary>
        public class ImruDriverTestType : IPartitionedInputDataSet, IEvaluatorRequestor
        {
            public int Count { get; private set; }
            public string Id { get; private set; }

            [Inject]
            public ImruDriverTestType()
            {
            }

            public IEnumerator<IPartitionDescriptor> GetEnumerator()
            {
                return new List<IPartitionDescriptor>().GetEnumerator();
            }

            IEnumerator IEnumerable.GetEnumerator()
            {
                return GetEnumerator();
            }

            public IPartitionDescriptor GetPartitionDescriptorForId(string partitionId)
            {
                throw new NotImplementedException();
            }

            public IResourceCatalog ResourceCatalog { get; private set; }
            public void Submit(IEvaluatorRequest request)
            {
                // nada
            }

            public EvaluatorRequestBuilder NewBuilder()
            {
                var builder = Activator.CreateInstance(
                    typeof(EvaluatorRequestBuilder), 
                    nonPublic: true);
                return builder as EvaluatorRequestBuilder;
            }

            public EvaluatorRequestBuilder NewBuilder(IEvaluatorRequest request)
            {
                return NewBuilder();
            }
        }
    }
}
