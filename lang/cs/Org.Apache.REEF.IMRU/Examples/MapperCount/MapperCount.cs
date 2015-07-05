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

using System.Linq;
using Org.Apache.REEF.IMRU.API;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Util;
using Org.Apache.REEF.Wake.StreamingCodec.CommonStreamingCodecs;

namespace Org.Apache.REEF.IMRU.Examples.MapperCount
{
    /// <summary>
    /// A simple IMRU program that counts the number of map function instances launched.
    /// </summary>
    public sealed class MapperCount
    {
        private readonly IIMRUClient<int, int, int> _imruClient;

        [Inject]
        private MapperCount(IIMRUClient<int, int, int> imruClient)
        {
            _imruClient = imruClient;
        }

        /// <summary>
        /// </summary>
        /// <returns>The number of MapFunction instances that are part of the job.</returns>
        public int Run()
        {
            var results = _imruClient.Submit(
                new IMRUJobDefinitionBuilder()
                    .SetConfiguration(
                        IMRUConfiguration<int, int, int>.ConfigurationModule
                            .Set(IMRUConfiguration<int, int, int>.MapFunction, GenericType<IdentityMapFunction>.Class)
                            .Set(IMRUConfiguration<int, int, int>.ReduceFunction,
                                GenericType<IntSumReduceFunction>.Class)
                            .Set(IMRUConfiguration<int, int, int>.UpdateFunction,
                                GenericType<MapperCountUpdateFunction>.Class)
                            .Set(IMRUConfiguration<int, int, int>.MapInputCodec,
                                GenericType<IntStreamingCodec>.Class)
                            .Set(IMRUConfiguration<int, int, int>.MapOutputCodec,
                                GenericType<IntStreamingCodec>.Class)
                            .Set(IMRUConfiguration<int, int, int>.ResultCodec, GenericType<IntStreamingCodec>.Class)
                            .Build())
                    .SetJobName("MapperCount")
                    .Build()
                );
            return results.First();
        }
    }
}