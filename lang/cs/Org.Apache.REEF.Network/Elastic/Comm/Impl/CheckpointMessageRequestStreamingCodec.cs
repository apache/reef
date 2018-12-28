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
using Org.Apache.REEF.Utilities;

namespace Org.Apache.REEF.Network.Elastic.Comm.Impl
{
    /// <summary>
    /// Streaming codec for the checkpoint message request
    /// </summary>
    internal sealed class CheckpointMessageRequestStreamingCodec : IStreamingCodec<CheckpointMessageRequest>
    {
        /// <summary>
        /// Empty constructor to allow instantiation by reflection.
        /// </summary>
        [Inject]
        private CheckpointMessageRequestStreamingCodec()
        {
        }

        /// <summary>
        /// Read the class fields.
        /// </summary>
        /// <param name="reader">The reader from which to read</param>
        /// <returns>The checkpoint message request</returns>
        public CheckpointMessageRequest Read(IDataReader reader)
        {
            int metadataSize = reader.ReadInt32() + sizeof(int) + sizeof(int);
            byte[] metadata = new byte[metadataSize];
            reader.Read(ref metadata, 0, metadataSize);
            var (stageName, operatorId, iteration) = GenerateMetaDataDecoding(metadata, metadataSize - sizeof(int) - sizeof(int));

            return new CheckpointMessageRequest(stageName, operatorId, iteration);
        }

        /// <summary>
        /// Writes the class fields.
        /// </summary>
        /// <param name="obj">The message to write</param>
        /// <param name="writer">The writer to which to write</param>
        public void Write(CheckpointMessageRequest obj, IDataWriter writer)
        {
            byte[] encodedMetadata = GenerateMetaDataEncoding(obj);

            writer.Write(encodedMetadata, 0, encodedMetadata.Length);
        }

        /// <summary>
        /// Read the class fields.
        /// </summary>
        /// <param name="reader">The reader from which to read </param>
        /// <param name="token">The cancellation token</param>
        /// <returns>The checkpoint message request</returns>
        public async Task<CheckpointMessageRequest> ReadAsync(IDataReader reader,
            CancellationToken token)
        {
            int metadataSize = reader.ReadInt32() + sizeof(int) + sizeof(int);
            byte[] metadata = new byte[metadataSize];
            await reader.ReadAsync(metadata, 0, metadataSize, token);
            var (stageName, operatorId, iteration) = GenerateMetaDataDecoding(metadata, metadataSize - sizeof(int) - sizeof(int));

            return new CheckpointMessageRequest(stageName, operatorId, iteration);
        }

        /// <summary>
        /// Writes the class fields.
        /// </summary>
        /// <param name="obj">The message to write</param>
        /// <param name="writer">The writer to which to write</param>
        /// <param name="token">The cancellation token</param>
        public async System.Threading.Tasks.Task WriteAsync(CheckpointMessageRequest obj, IDataWriter writer, CancellationToken token)
        {
            byte[] encodedMetadata = GenerateMetaDataEncoding(obj);

            await writer.WriteAsync(encodedMetadata, 0, encodedMetadata.Length, token);
        }

        private static byte[] GenerateMetaDataEncoding(CheckpointMessageRequest obj)
        {
            byte[] stageBytes = ByteUtilities.StringToByteArrays(obj.StageName);
            var length = stageBytes.Length;
            byte[] metadataBytes = new byte[sizeof(int) + length + sizeof(int) + sizeof(int)];
            int offset = 0;

            Buffer.BlockCopy(BitConverter.GetBytes(length), 0, metadataBytes, offset, sizeof(int));
            offset += sizeof(int);

            Buffer.BlockCopy(stageBytes, 0, metadataBytes, offset, length);
            offset += length;

            Buffer.BlockCopy(BitConverter.GetBytes(obj.OperatorId), 0, metadataBytes, offset, sizeof(int));
            offset += sizeof(int);

            Buffer.BlockCopy(BitConverter.GetBytes(obj.Iteration), 0, metadataBytes, offset, sizeof(int));

            return metadataBytes;
        }

        private static (string stageName, int operatorId, int iteration) GenerateMetaDataDecoding(byte[] obj, int stageLength)
        {
            int offset = 0;
            string stageName = ByteUtilities.ByteArraysToString(obj, offset, stageLength);
            offset += stageLength;

            int operatorId = BitConverter.ToInt32(obj, offset);
            offset += sizeof(int);

            int iteration = BitConverter.ToInt32(obj, offset);

            return (stageName, operatorId, iteration);
        }
    }
}