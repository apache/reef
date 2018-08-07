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
using Org.Apache.REEF.Bridge.Core.Common.Client.Config;
using Org.Apache.REEF.Bridge.Core.Common.Client.Config.Runtime;
using Org.Apache.REEF.Bridge.Core.Grpc.Client;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Tang.Util;
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.Bridge.Core.HelloREEF
{
    /// <summary>
    /// A Tool that submits HelloREEFDriver for execution.
    /// </summary>
    public sealed class HelloREEF
    {
        private static readonly Logger Log = Logger.GetLogger(typeof(HelloREEF));

        private const string Local = "local";
        private const string YARN = "yarn";
        private const string HDInsight = "hdi";
        private const string AzureBatch = "azurebatch";

        [Inject]
        private HelloREEF()
        {
        }

        /// <summary>
        /// Runs HelloREEF using the IREEFClient passed into the constructor.
        /// </summary>
        private void Run(string runtime)
        {
            // The driver configuration contains all the needed bindings.
            var helloDriverConfiguration = DriverApplicationConfiguration.ConfigurationModule
                .Set(DriverApplicationConfiguration.OnDriverStarted, GenericType<HelloDriver>.Class)
                .Set(DriverApplicationConfiguration.OnDriverStopped, GenericType<HelloDriver>.Class)
                .Set(DriverApplicationConfiguration.OnEvaluatorAllocated, GenericType<HelloDriver>.Class)
                .Set(DriverApplicationConfiguration.OnEvaluatorCompleted, GenericType<HelloDriver>.Class)
                .Set(DriverApplicationConfiguration.OnEvaluatorFailed, GenericType<HelloDriver>.Class)
                .Set(DriverApplicationConfiguration.OnContextActive, GenericType<HelloDriver>.Class)
                .Set(DriverApplicationConfiguration.OnTaskRunning, GenericType<HelloDriver>.Class)
                .Set(DriverApplicationConfiguration.OnTaskCompleted, GenericType<HelloDriver>.Class)
                .Set(DriverApplicationConfiguration.OnTaskFailed, GenericType<HelloDriver>.Class)
                .Set(DriverApplicationConfiguration.CustomTraceLevel, Level.Verbose.ToString())
                .Build();

            var applicationId = GetApplicationId();

            // The JobSubmission contains the Driver configuration as well as the files needed on the Driver.
            var driverRuntimeConfiguration = DriverRuntimeConfiguration.ConfigurationModule
                .Set(DriverRuntimeConfiguration.OsType, GenericType<OsLinux>.Class)
                .Set(DriverRuntimeConfiguration.JobId, applicationId);
            driverRuntimeConfiguration = DriverRuntimeConfiguration.AddGlobalAssemblyForType(driverRuntimeConfiguration, typeof(HelloDriver));

            var runtimeConfiguration = GetRuntimeConfiguration(runtime);

            using (var launcher = ClientLauncherFactory.GetLauncher(runtimeConfiguration, driverRuntimeConfiguration.Build()))
            {
                var launcherStatus = launcher.SubmitAsync(helloDriverConfiguration).Result;
                Log.Log(Level.Info, "Final Launch Status {0}", launcherStatus);
                if (launcherStatus.Error.IsPresent())
                {
                    throw launcherStatus.Error.Value;
                }
            }
        }

        private string GetApplicationId()
        {
            return "HelloWorldJob-" + DateTime.Now.ToString("ddd-MMM-d-HH-mm-ss-yyyy", CultureInfo.CreateSpecificCulture("en-US"));
        }

        /// <summary>
        /// </summary>
        /// <param name="name"></param>
        /// <returns></returns>
        private static IConfiguration GetRuntimeConfiguration(string name)
        {
            switch (name)
            {
                case Local:
                    return LocalRuntimeConfiguration.ConfigurationModule
                        .Set(LocalRuntimeConfiguration.NumberOfEvaluators, "2")
                        .Build();
                case YARN:
                    return YarnRuntimeConfiguration.ConfigurationModule.Build();
                case HDInsight:
                    // To run against HDInsight please replace placeholders below, with actual values for
                    // connection string, container name (available at Azure portal) and HDInsight
                    // credentials (username and password)
                    const string connectionString = "ConnString";
                    const string continerName = "foo";
                    return HdInsightRuntimeConfiguration.ConfigurationModule
                        .Set(HdInsightRuntimeConfiguration.HdInsightPassword, @"pwd")
                        .Set(HdInsightRuntimeConfiguration.HdInsightUserName, @"foo")
                        .Set(HdInsightRuntimeConfiguration.HdInsightUrl, @"https://foo.azurehdinsight.net/")
                        .Build();
                case AzureBatch:
                    return AzureBatchRuntimeConfiguration.ConfigurationModule
                        .Set(AzureBatchRuntimeConfiguration.AzureBatchAccountKey, "###")
                        .Set(AzureBatchRuntimeConfiguration.AzureBatchAccountName, "###")
                        .Set(AzureBatchRuntimeConfiguration.AzureBatchAccountUri, "###")
                        .Set(AzureBatchRuntimeConfiguration.AzureBatchPoolId, "###")
                        .Set(AzureBatchRuntimeConfiguration.AzureStorageAccountKey, "###")
                        .Set(AzureBatchRuntimeConfiguration.AzureStorageAccountName, "###")
                        .Set(AzureBatchRuntimeConfiguration.AzureStorageContainerName, "###")
                        .Build();
                default:
                    throw new Exception("Unknown runtime: " + name);
            }
        }

        public static void MainSimple(string[] args)
        {
            var runtime = args.Length > 0 ? args[0] : Local;

            // Execute the HelloREEF, with these parameters injected
            TangFactory.GetTang()
                .NewInjector()
                .GetInstance<HelloREEF>()
                .Run(runtime);
        }
    }
}
