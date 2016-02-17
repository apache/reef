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
using Org.Apache.REEF.Tang.Interface;

namespace Org.Apache.REEF.Client.API
{
    /// <summary>
    /// This interfaces provides all the information that is needed for 
    /// a job submission
    /// </summary>
    [Obsolete("Deprecated in 0.14. Please use JobRequest.")]
    public interface IJobSubmission
    {
        /// <summary>
        /// The assemblies to be made available to all containers.
        /// </summary>
        ISet<string> GlobalAssemblies { get; }

        /// <summary>
        /// The driver configurations
        /// </summary>
        ISet<IConfiguration> DriverConfigurations { get; }

        /// <summary>
        /// Global files. 
        /// </summary>
        ISet<string> GlobalFiles { get; }

        /// <summary>
        /// Local assemblies.
        /// </summary>
        ISet<string> LocalAssemblies { get; }

        /// <summary>
        /// Local files. 
        /// </summary>
        ISet<string> LocalFiles { get; }

        /// <summary>
        /// Driver memory in MegaBytes. 
        /// </summary>
        int DriverMemory { get; }

        /// <summary>
        /// The maximum amount of times an application can be submitted.
        /// </summary>
        int MaxApplicationSubmissions { get; }

        /// <summary>
        /// The Job's identifier
        /// </summary>
        string JobIdentifier { get; }

        /// <summary>
        /// Driver config file contents (Org.Apache.REEF.Bridge.exe.config) contents
        /// Can be used to redirect assembly versions
        /// </summary>
        string DriverConfigurationFileContents { get; }
    }
}
