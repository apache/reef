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
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Wake;
using Org.Apache.REEF.Wake.Remote;
using Org.Apache.REEF.Wake.StreamingCodec;

namespace Org.Apache.REEF.Network.NetworkService.Codec
{
    /// <summary>
    /// Codec to serialize NsMessages for NetworkService.
    /// </summary>
    /// <typeparam name="T">The message type</typeparam>
    internal class NsMessageStreamingCodec<T> : IStreamingCodec<NsMessage<T>>
    {
        private readonly IIdentifierFactory _idFactory;
        private readonly StreamingCodecFunctionCache<T> _codecFunctionsCache;

        /// <summary>
        /// Create new NsMessageCodec.
        /// </summary>
        /// <param name="idFactory">Used to create identifier from string.</param>
        /// <param name="injector">Injector to instantiate codecs.</param>
        [Inject]
        private NsMessageStreamingCodec(IIdentifierFactory idFactory, IInjector injector)
        {
            _idFactory = idFactory;
            _codecFunctionsCache = new StreamingCodecFunctionCache<T>(injector);
        }

        /// <summary>
        /// Instantiate the class from the reader.
        /// </summary>
        /// <param name="reader">The reader from which to read</param>
        /// <returns>The instance of type NsMessage<T> read from the reader</returns>
        public NsMessage<T> Read(IDataReader reader)
        {
            int metadataSize = reader.ReadInt32();
            byte[] metadata = new byte[metadataSize];
            reader.Read(ref metadata, 0, metadataSize);
            var res = GenerateMetaDataDecoding(metadata);

            Type messageType = res.Item3;
            NsMessage<T> message = res.Item1;

            var codecReadFunc = _codecFunctionsCache.ReadFunction(messageType);
            int messageCount = res.Item2;

            for (int i = 0; i < messageCount; i++)
            {
                message.Data.Add(codecReadFunc(reader));
            }

            return message;
        }

        /// <summary>
        /// Writes the class fields to the writer.
        /// </summary>
        /// <param name="obj">The object of type NsMessage<T> to be encoded</param>
        /// <param name="writer">The writer to which to write</param>
        public void Write(NsMessage<T> obj, IDataWriter writer)
        {
            byte[] encodedMetadata = GenerateMetaDataEncoding(obj);
            byte[] encodedInt = BitConverter.GetBytes(encodedMetadata.Length);
            byte[] totalEncoding = encodedInt.Concat(encodedMetadata).ToArray();
            writer.Write(totalEncoding, 0, totalEncoding.Length);

            Type messageType = obj.Data[0].GetType();        
            var codecWriteFunc = _codecFunctionsCache.WriteFunction(messageType);
          
            foreach (var data in obj.Data)
            {
                codecWriteFunc(data, writer);
            }
        }

        /// <summary>
        /// Instantiate the class from the reader.
        /// </summary>
        /// <param name="reader">The reader from which to read</param>
        /// <param name="token">Cancellation token</param>
        /// <returns>The instance of type NsMessage<T> read from the reader</returns>
        public async Task<NsMessage<T>> ReadAsync(IDataReader reader, CancellationToken token)
        {
            int metadataSize = await reader.ReadInt32Async(token);
            byte[] metadata = new byte[metadataSize];
            await reader.ReadAsync(metadata, 0, metadataSize, token);
            var res = GenerateMetaDataDecoding(metadata);
            Type messageType = res.Item3;
            NsMessage<T> message = res.Item1;
            var codecReadFunc = _codecFunctionsCache.ReadAsyncFunction(messageType);
            int messageCount = res.Item2;

            for (int i = 0; i < messageCount; i++)
            {
                message.Data.Add(codecReadFunc(reader, token));
            }

            return message;
        }

        /// <summary>
        /// Writes the class fields to the writer.
        /// </summary>
        /// <param name="obj">The object of type NsMessage<T> to be encoded</param>
        /// <param name="writer">The writer to which to write</param>
        /// <param name="token">Cancellation token</param>
        public async Task WriteAsync(NsMessage<T> obj, IDataWriter writer, CancellationToken token)
        {
            byte[] encodedMetadata = GenerateMetaDataEncoding(obj);
            byte[] encodedInt = BitConverter.GetBytes(encodedMetadata.Length);
            byte[] totalEncoding = encodedInt.Concat(encodedMetadata).ToArray();
            await writer.WriteAsync(totalEncoding, 0, totalEncoding.Length, token);

            Type messageType = obj.Data[0].GetType();

            var codecWriteFunc = _codecFunctionsCache.WriteAsyncFunction(messageType);

            foreach (var data in obj.Data)
            {
                var asyncResult = codecWriteFunc.BeginInvoke(data, writer, token, null, null);
                await codecWriteFunc.EndInvoke(asyncResult);
            }
        }

        private static byte[] GenerateMetaDataEncoding(NsMessage<T> obj)
        {
            List<byte[]> metadataBytes = new List<byte[]>();
            byte[] sourceBytes = StringToBytes(obj.SourceId.ToString());
            byte[] dstBytes = StringToBytes(obj.DestId.ToString());
            byte[] messageTypeBytes = StringToBytes(obj.Data[0].GetType().AssemblyQualifiedName);
            byte[] messageCount = BitConverter.GetBytes(obj.Data.Count);

            metadataBytes.Add(BitConverter.GetBytes(sourceBytes.Length));
            metadataBytes.Add(BitConverter.GetBytes(dstBytes.Length));
            metadataBytes.Add(BitConverter.GetBytes(messageTypeBytes.Length));
            metadataBytes.Add(sourceBytes);
            metadataBytes.Add(dstBytes);
            metadataBytes.Add(messageTypeBytes);
            metadataBytes.Add(messageCount);

            return metadataBytes.SelectMany(i => i).ToArray();
        }

        private Tuple<NsMessage<T>, int, Type> GenerateMetaDataDecoding(byte[] obj)
        {
            int srcCount = BitConverter.ToInt32(obj, 0);
            int dstCount = BitConverter.ToInt32(obj, sizeof(int));
            int msgTypeCount = BitConverter.ToInt32(obj, 2 * sizeof(int));

            int offset = 3 * sizeof(int);
            string srcString = BytesToString(obj.Skip(offset).Take(srcCount).ToArray());
            offset += srcCount;
            string dstString = BytesToString(obj.Skip(offset).Take(dstCount).ToArray());
            offset += dstCount;
            Type msgType = Type.GetType(BytesToString(obj.Skip(offset).Take(msgTypeCount).ToArray()));
            offset += msgTypeCount;
            int messageCount = BitConverter.ToInt32(obj, offset);

            NsMessage<T> msg = new NsMessage<T>(_idFactory.Create(srcString), _idFactory.Create(dstString));
            return new Tuple<NsMessage<T>, int, Type>(msg, messageCount, msgType);
        }

        private static byte[] StringToBytes(string str)
        {
            byte[] bytes = new byte[str.Length * sizeof(char)];
            Buffer.BlockCopy(str.ToCharArray(), 0, bytes, 0, bytes.Length);
            return bytes;
        }

        private static string BytesToString(byte[] bytes)
        {
            char[] chars = new char[bytes.Length / sizeof(char)];
            Buffer.BlockCopy(bytes, 0, chars, 0, bytes.Length);
            return new string(chars);
        }
    }
}