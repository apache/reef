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
using System.IO;
using Org.Apache.REEF.Tang.Interface;

namespace Org.Apache.REEF.Client.API
{
    /// <summary>
    /// Captures a submission of a REEF Job to a cluster.
    /// </summary>
    public sealed class JobSubmission
    {
        private readonly ISet<IConfiguration> _driverConfigurations = new HashSet<IConfiguration>();
        private readonly ISet<string> _globalAssemblies = new HashSet<string>();
        private readonly ISet<string> _globalFiles = new HashSet<string>();
        private readonly ISet<string> _localAssemblies = new HashSet<string>();
        private readonly ISet<string> _localFiles = new HashSet<string>();
        private int _driverMemory = 512;

        /// <summary>
        /// The assemblies to be made available to all containers.
        /// </summary>
        internal ISet<string> GlobalAssemblies
        {
            get { return _globalAssemblies; }
        }

        /// <summary>
        /// The driver configurations
        /// </summary>
        internal ISet<IConfiguration> DriverConfigurations
        {
            get { return _driverConfigurations; }
        }

        internal ISet<string> GlobalFiles
        {
            get { return _globalFiles; }
        }

        internal ISet<string> LocalAssemblies
        {
            get { return _localAssemblies; }
        }

        internal ISet<string> LocalFiles
        {
            get { return _localFiles; }
        }

        internal int DriverMemory
        {
            get { return _driverMemory; }
        }

        /// <summary>
        /// The Job's identifier
        /// </summary>
        public string JobIdentifier { get; private set; }

        /// <summary>
        /// Add a file to be made available in all containers.
        /// </summary>
        /// <param name="fileName"></param>
        /// <returns></returns>
        public JobSubmission AddGlobalFile(string fileName)
        {
            _globalFiles.Add(fileName);
            return this;
        }

        /// <summary>
        /// Add a file to be made available only on the driver.
        /// </summary>
        /// <param name="fileName"></param>
        /// <returns></returns>
        public JobSubmission AddLocalFile(string fileName)
        {
            _localFiles.Add(fileName);
            return this;
        }

        /// <summary>
        /// Add an assembly to be made available on all containers.
        /// </summary>
        /// <param name="fileName"></param>
        /// <returns></returns>
        public JobSubmission AddGlobalAssembly(string fileName)
        {
            _globalAssemblies.Add(fileName);
            return this;
        }

        /// <summary>
        /// Add an assembly to the driver only.
        /// </summary>
        /// <param name="fileName"></param>
        /// <returns></returns>
        public JobSubmission AddLocalAssembly(string fileName)
        {
            _localAssemblies.Add(fileName);
            return this;
        }

        /// <summary>
        /// Add a Configuration to the Driver.
        /// </summary>
        /// <param name="configuration"></param>
        /// <returns></returns>
        public JobSubmission AddDriverConfiguration(IConfiguration configuration)
        {
            _driverConfigurations.Add(configuration);
            return this;
        }

        /// <summary>
        /// Add the assembly needed for the given Type to the driver.
        /// </summary>
        /// <param name="type"></param>
        /// <returns></returns>
        public JobSubmission AddLocalAssemblyForType(Type type)
        {
            AddLocalAssembly(GetAssemblyPathForType(type));
            return this;
        }

        /// <summary>
        /// Add the assembly needed for the given Type to all containers.
        /// </summary>
        /// <param name="type"></param>
        /// <returns></returns>
        public JobSubmission AddGlobalAssemblyForType(Type type)
        {
            AddGlobalAssembly(GetAssemblyPathForType(type));
            return this;
        }

        /// <summary>
        /// Gives the job an identifier.
        /// </summary>
        /// <param name="id"></param>
        /// <returns></returns>
        public JobSubmission SetJobIdentifier(string id)
        {
            JobIdentifier = id;
            return this;
        }

        /// <summary>
        /// Sets the amount of memory (in MB) to allocate for the Driver.
        /// </summary>
        /// <param name="driverMemoryInMb">The amount of memory (in MB) to allocate for the Driver.</param>
        /// <returns>this</returns>
        public JobSubmission SetDriverMemory(int driverMemoryInMb)
        {
            _driverMemory = driverMemoryInMb;
            return this;
        }

        /// <summary>
        /// Finds the path to the assembly the given Type was loaded from.
        /// </summary>
        /// <param name="type"></param>
        /// <returns></returns>
        private static string GetAssemblyPathForType(Type type)
        {
            var path = Uri.UnescapeDataString(new UriBuilder(type.Assembly.CodeBase).Path);
            return Path.GetFullPath(path);
        }
    }
}