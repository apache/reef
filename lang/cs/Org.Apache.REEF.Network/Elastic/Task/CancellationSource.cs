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
using System.Threading;

namespace Org.Apache.REEF.Network.Elastic.Task
{
    /// <summary>
    /// Generic cancellation source for task operations.
    /// This class basically wraps <see cref="CancellationTokenSource"/> and uses Tang
    /// to inject the same source through the elastic communication services.
    /// </summary>
    [Unstable("0.16", "API may change")]
    internal sealed class CancellationSource
    {
        [Inject]
        public CancellationSource()
        {
            Source = new CancellationTokenSource();
        }

        /// <summary>
        /// The wrapped cancellation source.
        /// </summary>
        public CancellationTokenSource Source { get; private set; }

        /// <summary>
        /// Whether the operation is cancelled.
        /// </summary>
        /// <returns></returns>
        public bool IsCancelled
        {
            get { return Source.IsCancellationRequested; }
        }

        /// <summary>
        /// Cancel the currently running computation.
        /// </summary>
        public void Cancel()
        {
            Source.Cancel();
        }
    }
}