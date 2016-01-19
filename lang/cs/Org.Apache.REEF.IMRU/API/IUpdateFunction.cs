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

namespace Org.Apache.REEF.IMRU.API
{
    /// <summary>
    /// The interface for the Update function of IMRU
    /// </summary>
    /// <remarks>
    /// Objects implementing this interface are stateful.
    /// </remarks>
    /// <typeparam name="TMapInput">The type of the side information provided to the Map function</typeparam>
    /// <typeparam name="TMapOutput">The return type of the Map function</typeparam>
    /// <typeparam name="TResult">The return type of the computation.</typeparam>
    public interface IUpdateFunction<TMapInput, in TMapOutput, TResult>
    {
        /// <summary>
        /// The Update task for IMRU.
        /// </summary>
        /// <param name="input">The input produced by the IMapFunction instances after it was passed through the IReduceFunction.</param>
        /// <returns></returns>
        UpdateResult<TMapInput, TResult> Update(TMapOutput input);

        /// <summary>
        /// Called at the beginning of the computation.
        /// </summary>
        /// <returns></returns>
        UpdateResult<TMapInput, TResult> Initialize();
    }
}