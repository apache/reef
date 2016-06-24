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

namespace Org.Apache.REEF.IMRU.Examples
{
    /// <summary>
    /// A simple Update function for IMRU examples.
    /// </summary>
    /// <remarks>
    /// Upon Initialize(), this sends `1` to all Map Function instances. Each of them returns some result, which shows up as the
    /// parameter passed into `Update`. At that point, we can immediately terminate.
    /// </remarks>
    public sealed class SingleIterUpdateFunction : IUpdateFunction<int, int, int>
    {
        [Inject]
        private SingleIterUpdateFunction()
        {
        }

        /// <summary>
        /// Update function
        /// </summary>
        /// <param name="input">Input containing sum of all mapper results</param>
        /// <returns>The Update Result with only result</returns>
        public UpdateResult<int, int> Update(int input)
        {
            return UpdateResult<int, int>.Done(input);
        }

        /// <summary>
        /// Initialize function. Sends 1 to all mappers
        /// </summary>
        /// <returns>Map input</returns>
        public UpdateResult<int, int> Initialize()
        {
            return UpdateResult<int, int>.AnotherRound(1);
        }
    }
}