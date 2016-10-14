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

namespace Org.Apache.REEF.IMRU.API
{
    /// <summary>
    /// The interface is used by IMRU driver to detect if job was cancelled. 
    /// By default no cancellation events are implemented. 
    /// Client can set specific implementation of the interface when creating job definition.
    /// For instance IMRU job can be cancelled when client application is cancelled.
    /// </summary>
    [DefaultImplementation(typeof(JobCancellationDetectorAlwaysFalse))]
    public interface IJobCancelledDetector
    {
        /// <summary>
        /// Is used to check for cancellation signal
        /// </summary>
        /// <param name="cancellationMessage">details about cancellation event</param>
        /// <returns>true if cancellation signal is detected, false - otherwise</returns>
        bool IsJobCancelled(out string cancellationMessage);
    }
}
