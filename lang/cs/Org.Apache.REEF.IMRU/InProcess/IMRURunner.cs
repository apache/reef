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

using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using Org.Apache.REEF.IMRU.API;
using Org.Apache.REEF.Network.Group.Operators;
using Org.Apache.REEF.Tang.Annotations;

namespace Org.Apache.REEF.IMRU.InProcess
{
    /// <summary>
    /// Simple, single-threaded executor for IMRU Jobs.
    /// </summary>
    /// <typeparam name="TMapInput"></typeparam>
    /// <typeparam name="TMapOutput"></typeparam>
    /// <typeparam name="TResult"></typeparam>
    internal sealed class IMRURunner<TMapInput, TMapOutput, TResult>
    {
        private readonly ISet<IMapFunction<TMapInput, TMapOutput>> _mapfunctions;
        private readonly IReduceFunction<TMapOutput> _reduceTask;
        private readonly IUpdateFunction<TMapInput, TMapOutput, TResult> _updateTask;

        [Inject]
        private IMRURunner(MapFunctions<TMapInput, TMapOutput> mapfunctions,
            IReduceFunction<TMapOutput> reduceTask,
            IUpdateFunction<TMapInput, TMapOutput, TResult> updateTask)
        {
            _mapfunctions = mapfunctions.Mappers;
            _reduceTask = reduceTask;
            _updateTask = updateTask;
        }

        internal IList<TResult> Run()
        {
            var results = new List<TResult>();
            var updateResult = _updateTask.Initialize();

            while (updateResult.IsNotDone)
            {
                if (updateResult.HasResult)
                {
                    results.Add(updateResult.Result);
                }
                Debug.Assert(updateResult.HasMapInput);
                var mapinput = updateResult.MapInput;
                var mapOutputs = _mapfunctions.Select(x => x.Map(mapinput));
                var mapOutput = _reduceTask.Reduce(mapOutputs);
                updateResult = _updateTask.Update(mapOutput);
            }
            if (updateResult.HasResult)
            {
                results.Add(updateResult.Result);
            }
            return results;
        }
    }
}