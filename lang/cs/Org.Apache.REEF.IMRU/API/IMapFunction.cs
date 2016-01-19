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
    /// Interface to be implemented by Map functions in IMRU.
    /// </summary>
    /// <remarks>
    /// Objects of this type are stateful in the sense that the Map function will be called many times, and state can be
    /// kept in instance variables in between these invocations.
    /// The data the map is performed on is assumed to be passed in as a constructor parameter.
    /// </remarks>
    /// <typeparam name="TMapInput">The type of the side information provided to the Map function</typeparam>
    /// <typeparam name="TMapOutput">The return type of the Map function</typeparam>
    public interface IMapFunction<in TMapInput, out TMapOutput>
    {
        /// <summary>
        /// Computes new output based on the given side information and data.
        /// </summary>
        /// <param name="mapInput">Can't be null.</param>
        /// <returns>The output of this map round. Can't be null.</returns>
        TMapOutput Map(TMapInput mapInput);
    }
}