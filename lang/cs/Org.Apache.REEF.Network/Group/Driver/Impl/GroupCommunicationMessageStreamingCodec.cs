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
using System.Threading;
using System.Threading.Tasks;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Wake.Remote;
using Org.Apache.REEF.Wake.StreamingCodec;

namespace Org.Apache.REEF.Network.Group.Driver.Impl
{
    /// <summary>
    /// Streaming Codec for the Group Communication Message
    /// </summary>
    internal sealed class GroupCommunicationMessageStreamingCodec<T> : IStreamingCodec<GroupCommunicationMessage<T>>
    {
        private readonly IStreamingCodec<T> _codec;

        /// <summary>
        /// Empty constructor to allow instantiation by reflection
        /// </summary>
        [Inject]
        private GroupCommunicationMessageStreamingCodec(IStreamingCodec<T> codec)
        {
            _codec = codec;
        }

        /// <summary>
        /// Read the class fields.
        /// </summary>
        /// <param name="reader">The reader from which to read </param>
        /// <returns>The Group Communication Message</returns>
        public GroupCommunicationMessage<T> Read(IDataReader reader)
        {
            int metadataSize = reader.ReadInt32();
            byte[] metadata = new byte[metadataSize];
            reader.Read(ref metadata, 0, metadataSize);
            var res = GenerateMetaDataDecoding(metadata);

            string groupName = res.Item1;
            string operatorName = res.Item2;
            string source = res.Item3;
            string destination = res.Item4;
            int dataCount = res.Item5;

            if (dataCount == 0)
            {
                throw new Exception("Data Count in Group Communication Message cannot be zero");
            }

            var data = new T[dataCount];

            for (int index = 0; index < dataCount; index++)
            {
                data[index] = _codec.Read(reader);

                if (data[index] == null)
                {
                    throw new Exception("message instance cannot be created from the IDataReader in Group Communication Message");
                }
            }

            return new GroupCommunicationMessage<T>(groupName, operatorName, source, destination, data);
        }

        /// <summary>
        /// Writes the class fields.
        /// </summary>
        /// <param name="obj">The message to write</param>
        /// <param name="writer">The writer to which to write</param>
        public void Write(GroupCommunicationMessage<T> obj, IDataWriter writer)
        {
            byte[] encodedMetadata = GenerateMetaDataEncoding(obj);
            byte[] encodedInt = BitConverter.GetBytes(encodedMetadata.Length);
            byte[] totalEncoding = encodedInt.Concat(encodedMetadata).ToArray();
            writer.Write(totalEncoding, 0, totalEncoding.Length);

            foreach (var data in obj.Data)
            {
                _codec.Write(data, writer);
            }
        }

        /// <summary>
        /// Read the class fields.
        /// </summary>
        /// <param name="reader">The reader from which to read </param>
        /// <param name="token">The cancellation token</param>
        /// <returns>The Group Communication Message</returns>
        public async Task<GroupCommunicationMessage<T>> ReadAsync(IDataReader reader,
            CancellationToken token)
        {
            int metadataSize = await reader.ReadInt32Async(token);
            byte[] metadata = new byte[metadataSize];
            await reader.ReadAsync(metadata, 0, metadataSize, token);
            var res = GenerateMetaDataDecoding(metadata);

            string groupName = res.Item1;
            string operatorName = res.Item2;
            string source = res.Item3;
            string destination = res.Item4;
            int dataCount = res.Item5;

            if (dataCount == 0)
            {
                throw new Exception("Data Count in Group Communication Message cannot be zero");
            }

            var data = new T[dataCount];

            for (int index = 0; index < dataCount; index++)
            {
                data[index] = await _codec.ReadAsync(reader, token);

                if (data[index] == null)
                {
                    throw new Exception(
                        "message instance cannot be created from the IDataReader in Group Communication Message");
                }
            }

            return new GroupCommunicationMessage<T>(groupName, operatorName, source, destination, data);
        }

        /// <summary>
        /// Writes the class fields.
        /// </summary>
        /// <param name="obj">The message to write</param>
        /// <param name="writer">The writer to which to write</param>
        /// <param name="token">The cancellation token</param>
        public async System.Threading.Tasks.Task WriteAsync(GroupCommunicationMessage<T> obj, IDataWriter writer, CancellationToken token)
        {
            byte[] encodedMetadata = GenerateMetaDataEncoding(obj);
            byte[] encodedInt = BitConverter.GetBytes(encodedMetadata.Length);
            byte[] totalEncoding = encodedInt.Concat(encodedMetadata).ToArray();
            await writer.WriteAsync(totalEncoding, 0, totalEncoding.Length, token);

            foreach (var data in obj.Data)
            {
                await _codec.WriteAsync(data, writer, token);
            }
        }

        private static byte[] GenerateMetaDataEncoding(GroupCommunicationMessage<T> obj)
        {
            List<byte[]> metadataBytes = new List<byte[]>();

            byte[] groupBytes = StringToBytes(obj.GroupName);
            byte[] operatorBytes = StringToBytes(obj.OperatorName);
            byte[] sourceBytes = StringToBytes(obj.Source);
            byte[] dstBytes = StringToBytes(obj.Destination);
            byte[] messageCount = BitConverter.GetBytes(obj.Data.Length);

            metadataBytes.Add(BitConverter.GetBytes(groupBytes.Length));
            metadataBytes.Add(BitConverter.GetBytes(operatorBytes.Length));
            metadataBytes.Add(BitConverter.GetBytes(sourceBytes.Length));
            metadataBytes.Add(BitConverter.GetBytes(dstBytes.Length));
            metadataBytes.Add(groupBytes);
            metadataBytes.Add(operatorBytes);
            metadataBytes.Add(sourceBytes);
            metadataBytes.Add(dstBytes);
            metadataBytes.Add(messageCount);

            return metadataBytes.SelectMany(i => i).ToArray();
        }

        private static Tuple<string, string, string, string, int> GenerateMetaDataDecoding(byte[] obj)
        {
            int groupCount = BitConverter.ToInt32(obj, 0);
            int operatorCount = BitConverter.ToInt32(obj, sizeof(int));
            int srcCount = BitConverter.ToInt32(obj, 2 * sizeof(int));
            int dstCount = BitConverter.ToInt32(obj, 3 * sizeof(int));

            int offset = 4 * sizeof(int);

            string groupString = BytesToString(obj.Skip(offset).Take(groupCount).ToArray());
            offset += groupCount;
            string operatorString = BytesToString(obj.Skip(offset).Take(operatorCount).ToArray());
            offset += operatorCount;
            string srcString = BytesToString(obj.Skip(offset).Take(srcCount).ToArray());
            offset += srcCount;
            string dstString = BytesToString(obj.Skip(offset).Take(dstCount).ToArray());
            offset += dstCount;
            int messageCount = BitConverter.ToInt32(obj, offset);

            return new Tuple<string, string, string, string, int>(groupString, operatorString, srcString, dstString,
                messageCount);
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