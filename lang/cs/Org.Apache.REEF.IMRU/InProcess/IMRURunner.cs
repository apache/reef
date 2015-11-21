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
using System.IO;
using Org.Apache.REEF.IMRU.API;
using Org.Apache.REEF.Network.Group.Operators;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Wake.Remote.Impl;
using Org.Apache.REEF.Wake.StreamingCodec;

namespace Org.Apache.REEF.IMRU.InProcess
{
    /// <summary>
    /// Simple, single-threaded executor for IMRU Jobs.
    /// </summary>
    /// <typeparam name="TMapInput">Input to map function</typeparam>
    /// <typeparam name="TMapOutput">Output of map function</typeparam>
    /// <typeparam name="TResult">Final result</typeparam>
    internal sealed class IMRURunner<TMapInput, TMapOutput, TResult>
    {
        private readonly ISet<IMapFunction<TMapInput, TMapOutput>> _mapfunctions;
        private readonly IReduceFunction<TMapOutput> _reduceTask;
        private readonly IUpdateFunction<TMapInput, TMapOutput, TResult> _updateTask;
        private readonly IStreamingCodec<TMapInput> _mapInputCodec;
        private readonly IStreamingCodec<TMapOutput> _mapOutputCodec;

        [Inject]
        private IMRURunner(MapFunctions<TMapInput, TMapOutput> mapfunctions,
            IReduceFunction<TMapOutput> reduceTask,
            IUpdateFunction<TMapInput, TMapOutput, TResult> updateTask,
            InputCodecWrapper<TMapInput> mapInputCodec,
            OutputCodecWrapper<TMapOutput> mapOutputCodec)
        {
            _mapfunctions = mapfunctions.Mappers;
            _reduceTask = reduceTask;
            _updateTask = updateTask;
            _mapInputCodec = mapInputCodec.Codec;
            _mapOutputCodec = mapOutputCodec.Codec;
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

                Debug.Assert(updateResult.HasMapInput, "MapInput is needed.");
                var mapinput = updateResult.MapInput;
                var mapOutputs = new List<TMapOutput>();

                foreach (var mapfunc in _mapfunctions)
                {
                    // We create a copy by doing coding and decoding since the map task might 
                    // reuse the fields in next iteration and meanwhile update task might update it.
                    using (MemoryStream mapInputStream = new MemoryStream(), mapOutputStream = new MemoryStream())
                    {
                        var mapInputWriter = new StreamDataWriter(mapInputStream);
                        _mapInputCodec.Write(mapinput, mapInputWriter);
                        mapInputStream.Position = 0;
                        var mapInputReader = new StreamDataReader(mapInputStream);
                        var output = mapfunc.Map(_mapInputCodec.Read(mapInputReader));

                        var mapOutputWriter = new StreamDataWriter(mapOutputStream);
                        _mapOutputCodec.Write(output, mapOutputWriter);
                        mapOutputStream.Position = 0;
                        var mapOutputReader = new StreamDataReader(mapOutputStream);
                        output = _mapOutputCodec.Read(mapOutputReader);

                        mapOutputs.Add(output);
                    }
                }

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