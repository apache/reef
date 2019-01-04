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

using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Utilities.Attributes;
using System;

namespace Org.Apache.REEF.Network.Elastic.Task
{
    /// <summary>
    ///  Used by tasks to fetch the workflow of the stages configured in the driver.
    /// </summary>
    [Unstable("0.16", "API may change")]
    [DefaultImplementation(typeof(DefaultElasticStage))]
    public interface IElasticStage : IWaitForTaskRegistration, IDisposable
    {
        /// <summary>
        /// The name of the stage.
        /// </summary>
        string StageName { get; }

        /// <summary>
        /// Cacnel the execution of the stage.
        /// </summary>
        void Cancel();

        /// <summary>
        /// The workflow of operators.
        /// </summary>
        Workflow Workflow { get; }
    }
}
