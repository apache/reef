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
using System.Threading;
using System.Threading.Tasks;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Wake.Remote;
using Org.Apache.REEF.Wake.StreamingCodec;

namespace Org.Apache.REEF.IMRU.OnREEF.MapInputWithControlMessage
{
    /// <summary>
    /// Streaming codec for MapInputWithControlMessage
    /// </summary>
    /// <typeparam name="TMapInput"></typeparam>
    internal class MapInputWithControlMessageCodec<TMapInput> : IStreamingCodec<MapInputWithControlMessage<TMapInput>>
    {
        private readonly IStreamingCodec<TMapInput> _baseCodec;

        [Inject]
        private MapInputWithControlMessageCodec(IStreamingCodec<TMapInput> baseCodec)
        {
            _baseCodec = baseCodec;
        }

        /// <summary>
        /// Reads message from reader
        /// </summary>
        /// <param name="reader">reader from which to read the message</param>
        /// <returns>Read message</returns>
        public MapInputWithControlMessage<TMapInput> Read(IDataReader reader)
        {
            string controlMessageString = reader.ReadString();
            MapControlMessage controlMessage = (MapControlMessage)Enum.Parse(typeof (MapControlMessage), controlMessageString);

            if (controlMessage == MapControlMessage.AnotherRound)
            {
                TMapInput message = _baseCodec.Read(reader);
                return new MapInputWithControlMessage<TMapInput>(message, controlMessage);
            }

            return new MapInputWithControlMessage<TMapInput>(controlMessage);
        }

        /// <summary>
        /// Writes message to the writer
        /// </summary>
        /// <param name="obj">Message to write</param>
        /// <param name="writer">Writer used to write the message</param>
        public void Write(MapInputWithControlMessage<TMapInput> obj, IDataWriter writer)
        {
            writer.WriteString(obj.ControlMessage.ToString());

            if (obj.ControlMessage == MapControlMessage.AnotherRound)
            {
                _baseCodec.Write(obj.Message, writer);
            }
        }

        /// <summary>
        /// Reads message asynchronously from reader
        /// </summary>
        /// <param name="reader">reader from which to read the message</param>
        /// <param name="token">Cancellation token</param>
        /// <returns>Read message</returns>
        public async Task<MapInputWithControlMessage<TMapInput>> ReadAsync(IDataReader reader, CancellationToken token)
        {
            string controlMessageString = await reader.ReadStringAsync(token);
            MapControlMessage controlMessage = (MapControlMessage)Enum.Parse(typeof(MapControlMessage), controlMessageString);

            if (controlMessage == MapControlMessage.AnotherRound)
            {
                TMapInput message = await _baseCodec.ReadAsync(reader, token);
                return new MapInputWithControlMessage<TMapInput>(message, controlMessage);
            }

            return new MapInputWithControlMessage<TMapInput>(controlMessage);
        }

        /// <summary>
        /// Writes message asynchronously to the writer
        /// </summary>
        /// <param name="obj">Message to write</param>
        /// <param name="writer">Writer used to write the message</param>
        /// <param name="token">Cancellation token</param>
        public async Task WriteAsync(MapInputWithControlMessage<TMapInput> obj, IDataWriter writer, CancellationToken token)
        {
            await writer.WriteStringAsync(obj.ControlMessage.ToString(), token);

            if (obj.ControlMessage == MapControlMessage.AnotherRound)
            {
                await _baseCodec.WriteAsync(obj.Message, writer, token);
            }
        }
    }
}
