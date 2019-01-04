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
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Network.Elastic.Task.Impl;
using Org.Apache.REEF.Common.Tasks.Events;
using Org.Apache.REEF.Utilities.Attributes;

namespace Org.Apache.REEF.Network.Elastic.Task
{
    /// <summary>
    /// Used by REEF tasks to initialize group communication and fetch Stages.
    /// </summary>
    [Unstable("0.16", "API may change")]
    [DefaultImplementation(typeof(DefaultElasticContext))]
    public interface IElasticContext :
        IWaitForTaskRegistration,
        IDisposable,
        IObserver<ICloseEvent>
    {
        /// <summary>
        /// Gets the stage with the given name.
        /// </summary>
        /// <param name="stageName">The name of the stage</param>
        /// <returns>The task-side configured stage</returns>
        IElasticStage GetStage(string stageName);
    }
}
