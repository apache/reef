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
using System.Threading;
using Org.Apache.REEF.Bridge.Core.Common.Client.Config;
using Org.Apache.REEF.Bridge.Core.Grpc.Client;
using Org.Apache.REEF.Common.Client;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Tang.Util;

namespace Org.Apache.REEF.Bridge.Core.Tests.Fail.Task
{
    internal static class FailTaskClient
    {
        private static IConfiguration BuildAppDriverConfig(Type failTaskName)
        {
            IConfiguration driverConfig = DriverApplicationConfiguration.ConfigurationModule
                .Set(DriverApplicationConfiguration.OnDriverStarted, GenericType<Driver>.Class)
                .Set(DriverApplicationConfiguration.OnEvaluatorAllocated, GenericType<Driver>.Class)
                .Set(DriverApplicationConfiguration.OnContextActive, GenericType<Driver>.Class)
                .Set(DriverApplicationConfiguration.OnTaskRunning, GenericType<Driver>.Class)
                .Build();

            return TangFactory.GetTang().NewConfigurationBuilder(driverConfig)
                .BindNamedParameter(typeof(Driver.FailTaskName), failTaskName.Name)
                .Build();
        }

        /**
         * Run REEF on specified runtime and fail (raise an exception) in a specified class.
         * @param failTaskType A class that should fail during the test.
         * @param runtimeConfig REEF runtime configuration. Can be e.g. Local or YARN.
         * @param timeOut REEF application timeout.
         * @return launcher status - usually FAIL.
         * @throws InjectionException configuration error.
         */
        public static LauncherStatus Run(Type failTaskType, IConfiguration runtimeConfig, TimeSpan timeout)
        {
            var driverRuntimeConfiguration = DriverRuntimeConfiguration.ConfigurationModule
                .Set(DriverRuntimeConfiguration.OsType, GenericType<OsWindows>.Class)
                .Set(DriverRuntimeConfiguration.JobId, "Fail_" + failTaskType.Name);
            driverRuntimeConfiguration = DriverRuntimeConfiguration
                .AddGlobalAssemblyForType(driverRuntimeConfiguration, typeof(Driver));

            var source = new CancellationTokenSource();
            source.CancelAfter(timeout);
            using (var launcher = ClientLauncherFactory.GetLauncher(runtimeConfig, driverRuntimeConfiguration.Build()))
            {
                var task = launcher.SubmitAsync(BuildAppDriverConfig(failTaskType), source.Token);
                try
                {
                    return task.Result;
                }
                catch (Exception)
                {
                    if (task.IsCanceled)
                    {
                        throw new TimeoutException($"Job timed out after {timeout}");
                    }

                    throw;
                }
            }
        }
    }
}
