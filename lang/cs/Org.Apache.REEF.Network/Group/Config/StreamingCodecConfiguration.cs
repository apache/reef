﻿/**
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

using Org.Apache.REEF.Network.Group.Pipelining;
using Org.Apache.REEF.Network.StreamingCodec;
using Org.Apache.REEF.Tang.Formats;
using Org.Apache.REEF.Tang.Util;
using Org.Apache.REEF.Wake.Remote;

namespace Org.Apache.REEF.Network.Group.Config
{
    /// <summary>
    /// Defines configuration for streaming codecs and pipelie message 
    /// streaming codecs by taking streaming codec as input.
    /// </summary>
    /// <typeparam name="T">Generic type of message</typeparam>
    public sealed class StreamingCodecConfiguration<T> : ConfigurationModuleBuilder
    {
        /// <summary>
        /// RequiredImpl for Codec. Client needs to set implementation for this paramter
        /// </summary>
        public static readonly RequiredImpl<IStreamingCodec<T>> Codec = new RequiredImpl<IStreamingCodec<T>>();

        /// <summary>
        /// Configuration Module for Codec
        /// </summary>
        public static ConfigurationModule Conf = new StreamingCodecConfiguration<T>()
            .BindImplementation(GenericType<IStreamingCodec<T>>.Class, Codec)
            .BindImplementation(GenericType<IStreamingCodec<PipelineMessage<T>>>.Class, GenericType<StreamingPipelineMessageCodec<T>>.Class)
            .Build();
    }
}