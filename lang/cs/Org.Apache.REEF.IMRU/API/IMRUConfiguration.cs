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

using Org.Apache.REEF.IMRU.API.Parameters;
using Org.Apache.REEF.Network.Group.Operators;
using Org.Apache.REEF.Tang.Formats;
using Org.Apache.REEF.Tang.Util;
using Org.Apache.REEF.Wake.StreamingCodec;

namespace Org.Apache.REEF.IMRU.API
{
    /// <summary>
    /// A configuration module for IMRU.
    /// </summary>
    /// <typeparam name="TMapInput">The type of the side information provided to the Map function</typeparam>
    /// <typeparam name="TMapOutput">The return type of the Map function</typeparam>
    /// <typeparam name="TResult">The return type of the computation.</typeparam>
    public sealed class IMRUConfiguration<TMapInput, TMapOutput, TResult> : ConfigurationModuleBuilder
    {
        /// <summary>
        /// The codec to be used for the map input.
        /// </summary>
        public static readonly RequiredImpl<IStreamingCodec<TMapInput>> MapInputCodec =
            new RequiredImpl<IStreamingCodec<TMapInput>>();

        /// <summary>
        /// The codec to be used for the map output.
        /// </summary>
        public static readonly RequiredImpl<IStreamingCodec<TMapOutput>> MapOutputCodec =
            new RequiredImpl<IStreamingCodec<TMapOutput>>();

        /// <summary>
        /// The codec to be used for the result.
        /// </summary>
        public static readonly RequiredImpl<IStreamingCodec<TResult>> ResultCodec =
            new RequiredImpl<IStreamingCodec<TResult>>();

        /// <summary>
        /// The IReduceFunction type to use.
        /// </summary>
        public static readonly RequiredImpl<IReduceFunction<TMapOutput>> ReduceFunction =
            new RequiredImpl<IReduceFunction<TMapOutput>>();

        /// <summary>
        /// The IUpdateFunction type to use.
        /// </summary>
        public static readonly RequiredImpl<IUpdateFunction<TMapInput, TMapOutput, TResult>> UpdateFunction =
            new RequiredImpl<IUpdateFunction<TMapInput, TMapOutput, TResult>>();

        /// <summary>
        /// The IMapFunction type to use.
        /// </summary>
        public static readonly RequiredImpl<IMapFunction<TMapInput, TMapOutput>> MapFunction =
            new RequiredImpl<IMapFunction<TMapInput, TMapOutput>>();

        public static ConfigurationModule ConfigurationModule =
            new IMRUConfiguration<TMapInput, TMapOutput, TResult>()
                .BindNamedParameter(GenericType<MapInputCodec<TMapInput>>.Class, MapInputCodec)
                .BindNamedParameter(GenericType<MapOutputCodec<TMapOutput>>.Class, MapOutputCodec)
                .BindNamedParameter(GenericType<ResultCodec<TResult>>.Class, ResultCodec)
                .BindImplementation(GenericType<IReduceFunction<TMapOutput>>.Class, ReduceFunction)
                .BindImplementation(GenericType<IUpdateFunction<TMapInput, TMapOutput, TResult>>.Class, UpdateFunction)
                .BindImplementation(GenericType<IMapFunction<TMapInput, TMapOutput>>.Class, MapFunction)
                .Build();
    }
}