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

using System.Collections.Generic;
using Org.Apache.REEF.Network.Group.Operators;
using Org.Apache.REEF.Tang.Annotations;

namespace Org.Apache.REEF.IMRU.Examples.PipelinedBroadcastReduce
{
    /// <summary>
    /// A reduce function that sums integer arrays.
    /// </summary>
    internal sealed class IntArraySumReduceFunction : IReduceFunction<int[]>
    {
        [Inject]
        private IntArraySumReduceFunction()
        {
        }

        /// <summary>
        /// Reduce function that returns the sum of elements of int array
        /// </summary>
        /// <param name="elements">List of elements</param>
        /// <returns>The sum of elements</returns>
        int[] IReduceFunction<int[]>.Reduce(IEnumerable<int[]> elements)
        {
            int counter = 0;
            int[] resArr = null;

            foreach (var element in elements)
            {
                if (counter == 0)
                {
                    counter++;
                    resArr = new int[element.Length];
                }

                for (int i = 0; i < resArr.Length; i++)
                {
                    resArr[i] += element[i];
                }
            }

            return resArr;
        }
    }
}