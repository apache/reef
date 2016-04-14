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

using System.Collections.Generic;
using Org.Apache.REEF.Client.Common;
using Org.Apache.REEF.Utilities.Attributes;

namespace Org.Apache.REEF.IMRU.API
{
    public interface IIMRUClient
    {
        /// <summary>
        /// Submit the given job for execution.
        /// </summary>
        /// <typeparam name="TMapInput">The type of the side information provided to the Map function</typeparam>
        /// <typeparam name="TMapOutput">The return type of the Map function</typeparam>
        /// <typeparam name="TResult">The return type of the computation.</typeparam>
        /// <typeparam name="TDataHandler">Data Handler type</typeparam>
        /// <param name="jobDefinition">IMRU job definition</param>
        /// <returns>Result of IMRU</returns>
        IEnumerable<TResult> Submit<TMapInput, TMapOutput, TResult, TDataHandler>(IMRUJobDefinition jobDefinition);

        /// <summary>
        /// DriverHttpEndPoint returned by IReefClient after job submission
        /// returning null is a valid option for implementations that do not run on yarn or multi-core
        /// For example: see InProcessIMRUCLient.cs
        /// </summary>
        [Unstable("0.13", "This depends on IREEFClient API which itself in unstable ")]
        IJobSubmissionResult JobSubmissionResult { get; }
    }
}