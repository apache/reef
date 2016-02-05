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
using System.Linq;
using Org.Apache.REEF.Network.Group.Pipelining;
using Org.Apache.REEF.Tang.Annotations;

namespace Org.Apache.REEF.IMRU.OnREEF.MapInputWithControlMessage
{
    /// <summary>
    /// Pipeline Data Converter for MapInputwithControlMessage to chunk and dechunk the message for communication
    /// </summary>
    /// <typeparam name="TMapInput"></typeparam>
    internal sealed class MapInputwithControlMessagePipelineDataConverter<TMapInput> :
        IPipelineDataConverter<MapInputWithControlMessage<TMapInput>>
    {
        private readonly IPipelineDataConverter<TMapInput> _basePipelineDataConverter;

        [Inject]
        internal MapInputwithControlMessagePipelineDataConverter(
            IPipelineDataConverter<TMapInput> basePipelineDataConverter)
        {
            _basePipelineDataConverter = basePipelineDataConverter;
        }

        /// <summary>
        /// Chunks the message
        /// </summary>
        /// <param name="message">Message to be chunked</param>
        /// <returns>List of message chunks</returns>
        List<PipelineMessage<MapInputWithControlMessage<TMapInput>>>
            IPipelineDataConverter<MapInputWithControlMessage<TMapInput>>.PipelineMessage(
            MapInputWithControlMessage<TMapInput> message)
        {
            List<PipelineMessage<MapInputWithControlMessage<TMapInput>>> messageChunks =
                new List<PipelineMessage<MapInputWithControlMessage<TMapInput>>>();

            if (message.ControlMessage == MapControlMessage.Stop)
            {
                messageChunks.Add(new PipelineMessage<MapInputWithControlMessage<TMapInput>>(message, true));
                return messageChunks;
            }

            var baseMessageChunks = _basePipelineDataConverter.PipelineMessage(message.Message);

            messageChunks.AddRange(
                baseMessageChunks.Select(
                    t =>
                        new PipelineMessage<MapInputWithControlMessage<TMapInput>>(
                            new MapInputWithControlMessage<TMapInput>(t.Data, message.ControlMessage), t.IsLast)));

            return messageChunks;
        }

        /// <summary>
        /// Dechunks the message
        /// </summary>
        /// <param name="pipelineMessage">Message chunks</param>
        /// <returns>Single aggregated message</returns>
        MapInputWithControlMessage<TMapInput> IPipelineDataConverter<MapInputWithControlMessage<TMapInput>>.FullMessage(
            List<PipelineMessage<MapInputWithControlMessage<TMapInput>>> pipelineMessage)
        {
            if (pipelineMessage.Count == 1)
            {
                return pipelineMessage[0].Data;
            }

            var baseMessageChunks =
                pipelineMessage.Select(x => new PipelineMessage<TMapInput>(x.Data.Message, false)).ToList();

            MapInputWithControlMessage<TMapInput> combinedMessage =
                new MapInputWithControlMessage<TMapInput>(_basePipelineDataConverter.FullMessage(baseMessageChunks),
                    pipelineMessage[0].Data.ControlMessage);

            return combinedMessage;
        }
    }
}
