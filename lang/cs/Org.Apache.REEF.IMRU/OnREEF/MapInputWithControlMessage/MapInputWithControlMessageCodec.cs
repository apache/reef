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
using Org.Apache.REEF.Utilities.Logging;
using Org.Apache.REEF.Wake.Remote;
using Org.Apache.REEF.Wake.StreamingCodec;

namespace Org.Apache.REEF.IMRU.OnREEF.MapInputWithControlMessage
{
    /// <summary>
    /// Streaming codec for MapInputWithControlMessage
    /// </summary>
    /// <typeparam name="TMapInput"></typeparam>
    internal sealed class MapInputWithControlMessageCodec<TMapInput> : IStreamingCodec<MapInputWithControlMessage<TMapInput>>
    {
        private static readonly Logger Logger = Logger.GetLogger(typeof(MapInputWithControlMessage<>));
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
        MapInputWithControlMessage<TMapInput> IStreamingCodec<MapInputWithControlMessage<TMapInput>>.Read(
            IDataReader reader)
        {
            byte[] messageType = new byte[1];
            reader.Read(ref messageType, 0, 1);
            MapControlMessage controlMessage;

            switch (messageType[0])
            {
                case 0:
                    controlMessage = MapControlMessage.AnotherRound;
                    TMapInput message = _baseCodec.Read(reader);
                    return new MapInputWithControlMessage<TMapInput>(message, controlMessage);                   
                case 1:
                    controlMessage = MapControlMessage.Stop;
                    return new MapInputWithControlMessage<TMapInput>(controlMessage);
            }

            Utilities.Diagnostics.Exceptions.Throw(new Exception("Control message type not valid in Codec read"), Logger);
            return null;
        }

        /// <summary>
        /// Writes message to the writer
        /// </summary>
        /// <param name="obj">Message to write</param>
        /// <param name="writer">Writer used to write the message</param>
        void IStreamingCodec<MapInputWithControlMessage<TMapInput>>.Write(MapInputWithControlMessage<TMapInput> obj,
            IDataWriter writer)
        {
            switch (obj.ControlMessage)
            {
                case MapControlMessage.AnotherRound:
                    writer.Write(new byte[] { 0 }, 0, 1);
                    _baseCodec.Write(obj.Message, writer);
                    break;
                case MapControlMessage.Stop:
                    writer.Write(new byte[] { 1 }, 0, 1);
                    break;
            }
        }

        /// <summary>
        /// Reads message asynchronously from reader
        /// </summary>
        /// <param name="reader">reader from which to read the message</param>
        /// <param name="token">Cancellation token</param>
        /// <returns>Read message</returns>
        async Task<MapInputWithControlMessage<TMapInput>> IStreamingCodec<MapInputWithControlMessage<TMapInput>>.ReadAsync(
            IDataReader reader, CancellationToken token)
        {
            byte[] messageType = new byte[1];
            await reader.ReadAsync(messageType, 0, 1, token);
            MapControlMessage controlMessage = MapControlMessage.AnotherRound;

            switch (messageType[0])
            {
                case 0:
                    controlMessage = MapControlMessage.AnotherRound;
                    TMapInput message = await _baseCodec.ReadAsync(reader, token);
                    return new MapInputWithControlMessage<TMapInput>(message, controlMessage);
                case 1:
                    controlMessage = MapControlMessage.Stop;
                    return new MapInputWithControlMessage<TMapInput>(controlMessage);
            }

            Utilities.Diagnostics.Exceptions.Throw(new Exception("Control message type not valis in Codec read"), Logger);
            return null;
        }

        /// <summary>
        /// Writes message asynchronously to the writer
        /// </summary>
        /// <param name="obj">Message to write</param>
        /// <param name="writer">Writer used to write the message</param>
        /// <param name="token">Cancellation token</param>
        async Task IStreamingCodec<MapInputWithControlMessage<TMapInput>>.WriteAsync(
            MapInputWithControlMessage<TMapInput> obj, IDataWriter writer, CancellationToken token)
        {
            switch (obj.ControlMessage)
            {
                case MapControlMessage.AnotherRound:
                    await writer.WriteAsync(new byte[] { 0 }, 0, 1, token);
                    await _baseCodec.WriteAsync(obj.Message, writer, token);
                    break;
                case MapControlMessage.Stop:
                    writer.Write(new byte[] { 1 }, 0, 1);
                    break;
            }
        }
    }
}
