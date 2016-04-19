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
using System.Linq;

using Org.Apache.REEF.Client.Multi.Parameters;
using Org.Apache.REEF.Client.Yarn;
using Org.Apache.REEF.Client.YARN;
using Org.Apache.REEF.Client.YARN.HDI;
using Org.Apache.REEF.Client.YARN.Parameters;
using Org.Apache.REEF.Common.Runtime;
using Org.Apache.REEF.IO.FileSystem.AzureBlob;
using Org.Apache.REEF.Tang.Formats;
using Org.Apache.REEF.Tang.Implementations.Configuration;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Tang.Util;
using Org.Apache.REEF.Utilities;
using Org.Apache.REEF.Utilities.Collections;

namespace Org.Apache.REEF.Client.Multi
{
    /// <summary>
    /// Builder for a multi runtime configuration.
    /// Generates a configuration that instantiate a ReefClient that submits multi runtime job
    /// </summary>
    public sealed class MultiRuntimeConfigurationBuilder
    {
        private const string MultiRuntimeLauncher =
            "org.apache.reef.bridge.client.MultiRuntimeYarnBootstrapREEFLauncher";

        private static readonly ReadOnlySet<RuntimeName> SupportedRuntimes =
            new ReadOnlySet<RuntimeName>(new[] { RuntimeName.Yarn, RuntimeName.Local });

        private static readonly AvroConfigurationSerializer Serializer = new AvroConfigurationSerializer();

        /// <summary>
        /// List of runtime for evaluator submissions
        /// </summary>
        private ISet<RuntimeName> _runtimes = new HashSet<RuntimeName>();

        /// <summary>
        /// The default runtime to use for evaluator submission when none is specified
        /// </summary>
        private RuntimeName? _defaultRuntime = null;

        /// <summary>
        /// The submission runtime for the job, e.g. YarnRest/HDInsigh/Mesos etc.
        /// </summary>
        private SubmissionRuntimeName? _submissionRuntime;

        private int? _maxEvaluatorsNumber;

        private string _jobSubmissionDirectoryPrefix;

        private string _connectionString;

        private string _hdiUrl;

        private string _hdiUsername;

        private string _hdiPassword;

        /// <summary>
        /// Adds rutnime to teh defined runtimes list for the job. <code>ArgumentException</code>
        /// is thrown if non supported runtime is added. In case the same runtime is added several times only one value will be kept
        /// </summary>
        /// <param name="runtimeName">Runtime to add</param>
        /// <returns>The modified builder</returns>
        public MultiRuntimeConfigurationBuilder AddRuntime(RuntimeName runtimeName)
        {
            if (!SupportedRuntimes.Contains(runtimeName))
            {
                throw new ArgumentException("Unsupported runtime for multiruntime " + runtimeName);
            }

            _runtimes.Add(runtimeName);
            return this;
        }

        /// <summary>
        /// Sets the default runtime name. <code>ArgumentException</code> is thrown if unsupported runitme is specified
        /// or if set more tehn once.
        /// </summary>
        /// <param name="runtimeName">Default runtime name identifier</param>
        /// <returns>The modified builder</returns>
        public MultiRuntimeConfigurationBuilder SetDefaultRuntime(RuntimeName runtimeName)
        {
            if (!SupportedRuntimes.Contains(runtimeName))
            {
                throw new ArgumentException("Unsupported runtime for multiruntime " + runtimeName);
            }

            if (_defaultRuntime.HasValue)
            {
                throw new ArgumentException("Default runtime was already added");
            }

            _defaultRuntime = runtimeName;
            return this;
        }

        /// <summary>
        /// Sets the submission runtime <code>ArgumentException</code> is thrown is set more then once.
        /// </summary>
        /// <param name="runtimeName">The submission runtime identifier</param>
        /// <returns>The modified builder</returns>
        public MultiRuntimeConfigurationBuilder SetSubmissionRuntime(SubmissionRuntimeName runtimeName)
        {
            if (_submissionRuntime.HasValue)
            {
                throw new ArgumentException("Submission runtime was already set");
            }

            _submissionRuntime = runtimeName;
            return this;
        }

        /// <summary>
        /// Sets the max evaluators number. <code>ArgumentException</code> is thrown if non positive value provided
        /// </summary>
        /// <param name="maxEvaluatorsNumber">Max number of local evaluators</param>
        /// <returns>The modified builder</returns>
        public MultiRuntimeConfigurationBuilder SetMaxEvaluatorsNumberForLocalRuntime(int maxEvaluatorsNumber)
        {
            if (maxEvaluatorsNumber <= 0)
            {
                throw new ArgumentException("Max evaluators number should be a positive integer");
            }

            if (_maxEvaluatorsNumber.HasValue)
            {
                throw new ArgumentException("Max evaluators number was already set");
            }

            _maxEvaluatorsNumber = maxEvaluatorsNumber;
            return this;
        }

        /// <summary>
        /// Sets teh HDI user name
        /// </summary>
        /// <param name="userName">The user name</param>
        /// <returns>The modified builder</returns>
        public MultiRuntimeConfigurationBuilder SetHDInsightUsername(string userName)
        {
            _hdiUsername = userName;
            return this;
        }

        /// <summary>
        /// Sets teh HDI password
        /// </summary>
        /// <param name="pwd">The password</param>
        /// <returns>The modified builder</returns>
        public MultiRuntimeConfigurationBuilder SetHDInsightPassword(string pwd)
        {
            _hdiPassword = pwd;
            return this;
        }

        /// <summary>
        /// Sets the HDI URL
        /// </summary>
        /// <param name="hdiUrl">The URL to the HDI cluster endpoint</param>
        /// <returns>The modified builder</returns>
        public MultiRuntimeConfigurationBuilder SetHDInsightUrl(string hdiUrl)
        {
            _hdiUrl = hdiUrl;
            return this;
        }

        /// <summary>
        /// Azure Blobs Container name
        /// </summary>
        /// <param name="containerName">The container name</param>
        /// <returns>The modified builder</returns>
        public MultiRuntimeConfigurationBuilder SetAzureBlobContainerName(string containerName)
        {
            _jobSubmissionDirectoryPrefix = containerName;
            return this;
        }

        /// <summary>
        /// Azure blob connection string
        /// </summary>
        /// <param name="connectionString">The connection string</param>
        /// <returns>The modified builder</returns>
        public MultiRuntimeConfigurationBuilder SetAzureBlobConnectionString(string connectionString)
        {
            _connectionString = connectionString;
            return this;
        }

        /// <summary>
        /// Builds the configuration. <code>Argument exception is thrown if there are missing parameters</code>
        /// </summary>
        /// <returns>The modified builder</returns>
        public IConfiguration Build()
        {
            if (!_submissionRuntime.HasValue)
            {
                throw new ArgumentException("Submission runtime was not set");
            }

            if (!_defaultRuntime.HasValue && _runtimes.Count == 1)
            {
                _defaultRuntime = _runtimes.First();
            }

            if (!_defaultRuntime.HasValue)
            {
                throw new ArgumentException("Default runtime was not set");
            }

            if (_runtimes.Contains(RuntimeName.Local) && !_maxEvaluatorsNumber.HasValue)
            {
                throw new ArgumentException("Max evaluators number should be a positive integer");
            }

            IList<string> runtimes = _runtimes.Select(x => x.ToString()).ToList();
            var config = TangFactory.GetTang().NewConfigurationBuilder()
                .BindNamedParameter<DefaultRuntime, string>(GenericType<DefaultRuntime>.Class, _defaultRuntime.Value.ToString());

            foreach (var runtime in runtimes)
            {
                config.BindSetEntry<DefinedRuntimes, string>(GenericType<DefinedRuntimes>.Class, runtime);
            }
                
            var serializedConfig = Serializer.ToString(config.Build());
            var configBuilder = TangFactory.GetTang().NewConfigurationBuilder()
                .BindImplementation(
                    GenericType<IYarnREEFDotNetAppParamsSerializer>.Class,
                    GenericType<YarnMultiREEFDotNetAppParamSerializer>.Class)
                .BindNamedParameter<LauncherClassName, string>(
                        GenericType<LauncherClassName>.Class,
                        MultiRuntimeLauncher)
                .BindNamedParameter<AdditionalDriverAppConfiguration, string>(
                        GenericType<AdditionalDriverAppConfiguration>.Class,
                        serializedConfig);
           
            switch (_submissionRuntime.Value)
            {
                case SubmissionRuntimeName.HDInsight:
                    {
                        return GenerateHDIConfiguration(configBuilder);
                    }
                case SubmissionRuntimeName.YarnRest:
                    {
                        return GenerateYarnRestConfiguration(configBuilder);
                    }
                default:
                    throw new ArgumentException("Unsupported Submission Runitme " + _submissionRuntime.Value);
            }
        }

        private static IConfiguration GenerateYarnRestConfiguration(ICsConfigurationBuilder configBuilder)
        {
            return Configurations.Merge(configBuilder.Build(), YARNClientConfiguration.ConfigurationModuleYARNRest.Build());
        }

        private IConfiguration GenerateHDIConfiguration(ICsConfigurationBuilder configBuilder)
        {
            if (string.IsNullOrWhiteSpace(this._hdiUsername))
            {
                throw new ArgumentException("HDI user name was not set");
            }

            if (string.IsNullOrWhiteSpace(this._hdiPassword))
            {
                throw new ArgumentException("HDI password was not set");
            }

            if (string.IsNullOrWhiteSpace(this._hdiUrl))
            {
                throw new ArgumentException("HDI URL was not set");
            }

            if (string.IsNullOrWhiteSpace(this._jobSubmissionDirectoryPrefix))
            {
                throw new ArgumentException("Azure Blobs Container Name was not set");
            }

            if (string.IsNullOrWhiteSpace(this._connectionString))
            {
                throw new ArgumentException("Azure Blobs connection string was not set");
            }

            var hdiConfigurations =
                HDInsightClientConfiguration.ConfigurationModule.Set(
                    HDInsightClientConfiguration.HDInsightPasswordParameter,
                    this._hdiPassword)
                    .Set(HDInsightClientConfiguration.HDInsightUsernameParameter, this._hdiUsername)
                    .Set(HDInsightClientConfiguration.HDInsightUrlParameter, this._hdiUrl)
                    .Set(HDInsightClientConfiguration.JobSubmissionDirectoryPrefix, this._jobSubmissionDirectoryPrefix)
                    .Set(AzureBlockBlobFileSystemConfiguration.ConnectionString, this._connectionString)
                    .Build();
            return Configurations.Merge(configBuilder.Build(), hdiConfigurations);
        }
    }
}
