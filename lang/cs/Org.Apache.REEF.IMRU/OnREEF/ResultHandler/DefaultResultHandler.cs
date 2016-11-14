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

using Org.Apache.REEF.IMRU.API;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Utilities.Attributes;

namespace Org.Apache.REEF.IMRU.OnREEF.ResultHandler
{
    [Unstable("0.14", "This API will change after introducing proper API for output in REEF.IO")]
    internal sealed class DefaultResultHandler<TResult> : IIMRUResultHandler<TResult>
    {
        [Inject]
        private DefaultResultHandler()
        {
        }

        /// <summary>
        /// Specifies how to handle the IMRU results from UpdateTask. Does nothing
        /// </summary>
        /// <param name="result">The result of IMRU</param>
        public void HandleResult(TResult result)
        {
        }

        /// <summary>
        /// Handles what to do on completion
        /// In this case do nothing
        /// </summary>
        public void Dispose()
        {
        }
    }
}
