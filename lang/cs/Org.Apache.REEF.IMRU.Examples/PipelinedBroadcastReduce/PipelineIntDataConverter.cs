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

using System;
using System.Collections.Generic;
using System.Linq;
using Org.Apache.REEF.Network.Group.Pipelining;
using Org.Apache.REEF.Tang.Annotations;

namespace Org.Apache.REEF.IMRU.Examples.PipelinedBroadcastReduce
{
    /// <summary>
    /// Pipeline Data Converter for integer array
    /// </summary>
    internal class PipelineIntDataConverter : IPipelineDataConverter<int[]>
    {
        private readonly int _chunkSize;

        [Inject]
        private PipelineIntDataConverter([Parameter(typeof(BroadcastReduceConfiguration.ChunkSize))] int chunkSize)
        {
            _chunkSize = chunkSize;
        }

        /// <summary>
        /// Convert a integer array to chunks
        /// </summary>
        /// <param name="message">message to be chunked</param>
        /// <returns>chunked integer array</returns>
        List<PipelineMessage<int[]>> IPipelineDataConverter<int[]>.PipelineMessage(int[] message)
        {
            var messageList = new List<PipelineMessage<int[]>>();
            var totalChunks = message.Length / _chunkSize;

            if (message.Length % _chunkSize != 0)
            {
                totalChunks++;
            }

            var counter = 0;
            for (var i = 0; i < message.Length; i += _chunkSize)
            {
                var data = new int[Math.Min(_chunkSize, message.Length - i)];
                Buffer.BlockCopy(message, i * sizeof(int), data, 0, data.Length * sizeof(int));

                messageList.Add(counter == totalChunks - 1
                    ? new PipelineMessage<int[]>(data, true)
                    : new PipelineMessage<int[]>(data, false));

                counter++;
            }

            return messageList;
        }

        /// <summary>
        /// Converts integer array chunks to integer array
        /// </summary>
        /// <param name="pipelineMessage">List of integer array chunks</param>
        /// <returns>aggregated integer array</returns>
        int[] IPipelineDataConverter<int[]>.FullMessage(List<PipelineMessage<int[]>> pipelineMessage)
        {
            var size = pipelineMessage.Select(x => x.Data.Length).Sum();
            var data = new int[size];
            var offset = 0;

            foreach (var message in pipelineMessage)
            {
                Buffer.BlockCopy(message.Data, 0, data, offset, message.Data.Length * sizeof(int));
                offset += message.Data.Length * sizeof(int);
            }

            return data;
        }
    }
}
