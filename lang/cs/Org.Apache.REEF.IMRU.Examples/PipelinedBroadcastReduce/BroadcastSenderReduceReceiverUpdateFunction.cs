/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

using System;
using Org.Apache.REEF.IMRU.API;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Utilities.Diagnostics;
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.IMRU.Examples.PipelinedBroadcastReduce
{
    /// <summary>
    /// The Update function for integer array broadcast and reduce
    /// </summary>
    internal sealed class BroadcastSenderReduceReceiverUpdateFunction : IUpdateFunction<int[], int[], int[]>
    {
        private static readonly Logger Logger = Logger.GetLogger(typeof(BroadcastSenderReduceReceiverUpdateFunction));

        private int _iterations;
        private readonly int _maxIters;
        private readonly int _dim;
        private readonly int[] _intArr;
        private readonly int _workers;

        [Inject]
        private BroadcastSenderReduceReceiverUpdateFunction(
            [Parameter(typeof(BroadcastReduceConfiguration.NumberOfIterations))] int maxIters,
            [Parameter(typeof(BroadcastReduceConfiguration.Dimensions))] int dim,
            [Parameter(typeof(BroadcastReduceConfiguration.NumWorkers))] int numWorkers)
        {
            _maxIters = maxIters;
            _iterations = 0;
            _dim = dim;
            _intArr = new int[_dim];
            _workers = numWorkers;
        }

        /// <summary>
        /// Update function
        /// </summary>
        /// <param name="input">Input containing sum of all mappers arrays</param>
        /// <returns>The Update Result</returns>
        UpdateResult<int[], int[]> IUpdateFunction<int[], int[], int[]>.Update(int[] input)
        {
            Logger.Log(Level.Info, string.Format("Received value {0}", input[0]));

            if (input[0] != (_iterations + 1) * _workers)
            {
                Exceptions.Throw(new Exception("Expected input to update functon not same as actual input"), Logger);
            }

            _iterations++;

            if (_iterations < _maxIters)
            {
                for (int i = 0; i < _dim; i++)
                {
                    _intArr[i] = _iterations + 1;
                }

                return UpdateResult<int[], int[]>.AnotherRound(_intArr);
            }

            return UpdateResult<int[], int[]>.Done(input);
        }

        /// <summary>
        /// Initialize function. Sends integer array with value 1 to all mappers
        /// </summary>
        /// <returns>Map input</returns>
        UpdateResult<int[], int[]> IUpdateFunction<int[], int[], int[]>.Initialize()
        {
            for (int i = 0; i < _dim; i++)
            {
                _intArr[i] = _iterations + 1;
            }

            return UpdateResult<int[], int[]>.AnotherRound(_intArr);
        }
    }
}