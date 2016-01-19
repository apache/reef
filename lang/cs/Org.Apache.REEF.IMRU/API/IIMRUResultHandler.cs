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
using Org.Apache.REEF.Utilities.Attributes;

namespace Org.Apache.REEF.IMRU.API
{
    /// <summary>
    /// Interface defining how to handle the output of Update function in IMRU
    /// </summary>
    /// <typeparam name="T">Result type</typeparam>
    [Unstable("0.14", "This API will change after introducing proper API for output in REEF.IO")]
    public interface IIMRUResultHandler<in T> : IDisposable
    {
        /// <summary>
        /// Handles the result of IMRU updata function.
        /// For example, do nothing, write to file etc.
        /// </summary>
        /// <param name="result">The output of IMRU update function</param>
        void HandleResult(T result);
    }
}
