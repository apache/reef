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
using Org.Apache.REEF.Common.Client.Parameters;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Interface;

namespace Org.Apache.REEF.Client.API
{
    public sealed class JobRequestBuilder
    {
        private readonly AppParametersBuilder _appParametersBuilder = AppParametersBuilder.NewBuilder();
        private readonly JobParametersBuilder _jobParametersBuilder = JobParametersBuilder.NewBuilder();

        private JobRequestBuilder()
        {
        }

        [Inject]
        private JobRequestBuilder([Parameter(typeof(DriverConfigurationProviders))] ISet<IConfigurationProvider> configurationProviders)
        {
            AddDriverConfigurationProviders(configurationProviders);
        }

        public static JobRequestBuilder NewBuilder()
        {
            return new JobRequestBuilder();
        }

        /// <summary>
        /// Bake the information provided so far and return a IJobSubmission 
        /// </summary>
        public JobRequest Build()
        {
            return new JobRequest(_jobParametersBuilder.Build(), _appParametersBuilder.Build());
        }

        /// <summary>
        /// Make this file available to all containers
        /// </summary>
        public JobRequestBuilder AddGlobalFile(string fileName)
        {
            _appParametersBuilder.AddGlobalFile(fileName);
            return this;
        }

        /// <summary>
        /// Files specific to one container
        /// </summary>
        public JobRequestBuilder AddLocalFile(string fileName)
        {
            _appParametersBuilder.AddLocalFile(fileName);
            return this;
        }

        /// <summary>
        /// Add an assembly to be made available on all containers.
        /// </summary>
        /// <param name="fileName"></param>
        /// <returns></returns>
        public JobRequestBuilder AddGlobalAssembly(string fileName)
        {
            _appParametersBuilder.AddGlobalAssembly(fileName);
            return this;
        }

        /// <summary>
        /// Add an assembly to the driver only.
        /// </summary>
        /// <param name="fileName"></param>
        /// <returns></returns>
        public JobRequestBuilder AddLocalAssembly(string fileName)
        {
            _appParametersBuilder.AddLocalAssembly(fileName);
            return this;
        }

        /// <summary>
        /// Add a Configuration to the Driver.
        /// </summary>
        /// <param name="configuration"></param>
        /// <returns></returns>
        public JobRequestBuilder AddDriverConfiguration(IConfiguration configuration)
        {
            _appParametersBuilder.AddDriverConfiguration(configuration);
            return this;
        }

        /// <summary>
        /// Add the assembly needed for the given Type to the driver.
        /// </summary>
        /// <param name="type"></param>
        /// <returns></returns>
        public JobRequestBuilder AddLocalAssemblyForType(Type type)
        {
            _appParametersBuilder.AddLocalAssemblyForType(type);
            return this;
        }

        /// <summary>
        /// Add the assembly needed for the given Type to all containers.
        /// </summary>
        /// <param name="type"></param>
        /// <returns></returns>
        public JobRequestBuilder AddGlobalAssemblyForType(Type type)
        {
            _appParametersBuilder.AddGlobalAssemblyForType(type);
            return this;
        }

        /// <summary>
        /// Gives the job an identifier.
        /// </summary>
        /// <param name="id"></param>
        /// <returns></returns>
        public JobRequestBuilder SetJobIdentifier(string id)
        {
            _jobParametersBuilder.SetJobIdentifier(id);
            return this;
        }

        /// <summary>
        /// Sets the amount of memory (in MB) to allocate for the Driver.
        /// </summary>
        /// <param name="driverMemoryInMb">The amount of memory (in MB) to allocate for the Driver.</param>
        /// <returns>this</returns>
        public JobRequestBuilder SetDriverMemory(int driverMemoryInMb)
        {
            _jobParametersBuilder.SetDriverMemory(driverMemoryInMb);
            return this;
        }

        /// <summary>
        /// Sets the maximum amount of times a job can be submitted.
        /// </summary>
        public JobRequestBuilder SetMaxApplicationSubmissions(int maxAppSubmissions)
        {
            _jobParametersBuilder.SetMaxApplicationSubmissions(maxAppSubmissions);
            return this;
        }

        /// <summary>
        /// Driver config file contents (Org.Apache.REEF.Bridge.exe.config) contents
        /// Can be use to redirect assembly versions
        /// </summary>
        /// <param name="driverConfigurationFileContents">Driver configuration file contents.</param>
        /// <returns>this</returns>
        public JobRequestBuilder SetDriverConfigurationFileContents(string driverConfigurationFileContents)
        {
            _appParametersBuilder.SetDriverConfigurationFileContents(driverConfigurationFileContents);
            return this;
        }

        public JobRequestBuilder AddDriverConfigurationProviders(IEnumerable<IConfigurationProvider> configurationProviders)
        {
            _appParametersBuilder.AddDriverConfigurationProviders(configurationProviders);
            return this;
        }
    }
}
