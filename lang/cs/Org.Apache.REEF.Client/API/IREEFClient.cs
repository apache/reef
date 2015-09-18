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
using Org.Apache.REEF.Client.Common;
using Org.Apache.REEF.Common.Attributes;

namespace Org.Apache.REEF.Client.API
{
    /// <summary>
    /// Interface for job submission on a REEF cluster
    /// </summary>
    // ReSharper disable once InconsistentNaming
    public interface IREEFClient
    {
        /// <summary>
        /// Submit the job described in jobSubmission to the cluster.
        /// </summary>
        /// <param name="jobSubmission"></param>
        void Submit(IJobSubmission jobSubmission);

        /// <summary>
        /// Submit the job described in jobSubmission to the cluster.
        /// Expect IDriverHttpEndpoint returned after the call.
        /// </summary>
        /// <param name="jobSubmission"></param>
        /// <returns>IDriverHttpEndpoint</returns>
        [Unstable("0.13", "Working in progress for what to return after submit")]
        IDriverHttpEndpoint SubmitAndGetDriverUrl(IJobSubmission jobSubmission);
    }
}