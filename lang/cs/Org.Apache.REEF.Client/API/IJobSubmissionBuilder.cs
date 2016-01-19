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
using Org.Apache.REEF.Tang.Interface;

namespace Org.Apache.REEF.Client.API
{
    /// <summary>
    /// Facilitates building of job submissions
    /// </summary>
    public interface IJobSubmissionBuilder
    {
        /// <summary>
        /// Bake the information provided so far and return a IJobSubmission 
        /// </summary>
        IJobSubmission Build();

        /// <summary>
        /// Make this file available to all containers
        /// </summary>
        IJobSubmissionBuilder AddGlobalFile(string fileName);

        /// <summary>
        /// Files specific to one container
        /// </summary>
        IJobSubmissionBuilder AddLocalFile(string fileName);

        /// <summary>
        /// Assemblies available to all containers
        /// </summary>
        IJobSubmissionBuilder AddGlobalAssembly(string fileName);

        /// <summary>
        /// Assemblies available to a specific container
        /// </summary>
        IJobSubmissionBuilder AddLocalAssembly(string fileName);

        /// <summary>
        /// Configuration that will be available to the driver
        /// </summary>
        IJobSubmissionBuilder AddDriverConfiguration(IConfiguration configuration);

        /// <summary>
        /// Find the assembly for this type and make it available to a specific container 
        /// </summary>
        IJobSubmissionBuilder AddLocalAssemblyForType(Type type);

        /// <summary>
        /// Find the assembly for this type and make it available to all containers
        /// </summary>
        IJobSubmissionBuilder AddGlobalAssemblyForType(Type type);

        /// <summary>
        /// Specify job identifier.
        /// </summary>
        IJobSubmissionBuilder SetJobIdentifier(string id);

        /// <summary>
        /// Set driver memory in megabytes
        /// </summary>
        IJobSubmissionBuilder SetDriverMemory(int driverMemoryInMb);

        /// <summary>
        /// Driver config file contents (Org.Apache.REEF.Bridge.exe.config) contents
        /// Can be used to redirect assembly versions
        /// </summary>
        /// <param name="driverConfigurationFileContents">Driver configuration file contents.</param>
        /// <returns>IJobSubmissionBuilder</returns>
        IJobSubmissionBuilder SetDriverConfigurationFileContents(string driverConfigurationFileContents);
    }
}