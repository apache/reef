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

namespace Org.Apache.REEF.IMRU.Examples.NaturalSum
{
    /// <summary>
    /// Map function that returns its mapper id as output.
    /// </summary>
    internal sealed class NaturalSumMapFunction : IMapFunction<int, int>
    {
        private readonly int _mapperId;

        [Inject]
        private NaturalSumMapFunction([Parameter(typeof(NaturalSumPerMapperConfigGenerator.MapperId))] int mapperId)
        {
            _mapperId = mapperId;
        }

        /// <summary>
        /// Map function that ignores the input and returns this mapper's id as output
        /// </summary>
        /// <param name="mapInput">input given from the Update function, but is not used</param>
        /// <returns>id of this mapper</returns>
        public int Map(int mapInput)
        {
            return _mapperId;
        }
    }
}
