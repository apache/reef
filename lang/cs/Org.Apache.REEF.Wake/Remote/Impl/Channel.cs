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
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;

namespace Org.Apache.REEF.Wake.Remote.Impl
{
    /// <summary>
    /// Performs low level network IO operations between hosts
    /// </summary>
    public sealed class Channel
    {
        private readonly NetworkStream _stream;

        /// <summary>
        /// Constructs a new Channel with the the connected NetworkStream.
        /// </summary>
        /// <param name="stream">The connected stream</param>
        public Channel(NetworkStream stream)
        {
            if (stream == null)
            {
                throw new ArgumentNullException("stream");
            }

            _stream = stream;
        }

        /// <summary>
        /// Sends a message to the connected client synchronously
        /// </summary>
        /// <param name="message">The message to send</param>
        public void Write(byte[] message)
        {
            if (message == null)
            {
                throw new ArgumentNullException("message");
            }

            byte[] messageBuffer = GenerateMessageBuffer(message);
            _stream.Write(messageBuffer, 0, messageBuffer.Length);
        }

        /// <summary>
        /// Sends a message to the connected client asynchronously
        /// </summary>
        /// <param name="message">The message to send</param>
        /// <param name="token">The cancellation token</param>
        /// <returns>The awaitable write task</returns>
        public async Task WriteAsync(byte[] message, CancellationToken token)
        {
            byte[] messageBuffer = GenerateMessageBuffer(message);
            await _stream.WriteAsync(messageBuffer, 0, messageBuffer.Length, token);
        }

        /// <summary>
        /// Reads an incoming message as a byte array synchronously.
        /// The message length is read as the first four bytes.
        /// </summary>
        /// <returns>The byte array message</returns>
        public byte[] Read()
        {
            int payloadLength = ReadMessageLength();
            if (payloadLength == 0)
            {
                return null;
            }

            return ReadBytes(payloadLength);
        }

        /// <summary>
        /// Reads an incoming message as a byte array asynchronously.
        /// The message length is read as the first four bytes.
        /// </summary>
        /// <param name="token">The cancellation token</param>
        /// <returns>The byte array message</returns>
        public async Task<byte[]> ReadAsync(CancellationToken token)
        {
            int payloadLength = await GetMessageLengthAsync(token);
            if (payloadLength == 0)
            {
                return null;
            }

            return await ReadBytesAsync(payloadLength, token);
        }

        /// <summary>
        /// Helper method to read the specified number of bytes from the network stream.
        /// </summary>
        /// <param name="bytesToRead">The number of bytes to read</param>
        /// <returns>The byte[] read from the network stream with the requested 
        /// number of bytes, otherwise null if the operation failed.
        /// </returns>
        private byte[] ReadBytes(int bytesToRead)
        {
            int totalBytesRead = 0;
            byte[] buffer = new byte[bytesToRead];

            while (totalBytesRead < bytesToRead)
            {
                int bytesRead = _stream.Read(buffer, totalBytesRead, bytesToRead - totalBytesRead);
                if (bytesRead == 0)
                {
                    // Read timed out or connection was closed
                    return null;
                }

                totalBytesRead += bytesRead;
            }

            return buffer;
        }

        /// <summary>
        /// Helper method to read the specified number of bytes from the network stream.
        /// </summary>
        /// <param name="bytesToRead">The number of bytes to read</param>
        /// <param name="token">The cancellation token</param>
        /// <returns>The byte[] read from the network stream with the requested 
        /// number of bytes, otherwise null if the operation failed.
        /// </returns>
        private async Task<byte[]> ReadBytesAsync(int bytesToRead, CancellationToken token)
        {
            int bytesRead = 0;
            byte[] buffer = new byte[bytesToRead];

            while (bytesRead < bytesToRead)
            {
                int amountRead = await _stream.ReadAsync(buffer, bytesRead, bytesToRead - bytesRead, token);
                if (amountRead == 0)
                {
                    // Read timed out or connection was closed
                    return null;
                }

                bytesRead += amountRead;
            }

            return buffer;
        }

        /// <summary>
        /// Generates the payload buffer containing the message along
        /// with a header indicating the message length.
        /// </summary>
        /// <param name="message">The message to send</param>
        /// <returns>The payload buffer</returns>
        private byte[] GenerateMessageBuffer(byte[] message)
        {
            byte[] lengthBuffer1 = BitConverter.GetBytes(message.Length + 4);
            byte[] lengthBuffer2 = BitConverter.GetBytes(message.Length);
            if (BitConverter.IsLittleEndian)
            {
                Array.Reverse(lengthBuffer1);
            }

            int len = lengthBuffer1.Length + lengthBuffer2.Length + message.Length;
            byte[] messageBuffer = new byte[len];

            int bytesCopied = 0;
            bytesCopied += CopyBytes(lengthBuffer1, messageBuffer, 0);
            bytesCopied += CopyBytes(lengthBuffer2, messageBuffer, bytesCopied);
            CopyBytes(message, messageBuffer, bytesCopied);

            return messageBuffer;
        }

        /// <summary>
        /// Reads the first four bytes from the stream and decode
        /// it to get the message length in bytes
        /// </summary>
        /// <returns>The incoming message's length in bytes</returns>
        private int ReadMessageLength()
        {
            byte[] lenBytes = ReadBytes(sizeof(int));
            if (lenBytes == null)
            {
                return 0;
            }
            if (BitConverter.IsLittleEndian)
            {
                Array.Reverse(lenBytes);
            }
            if (BitConverter.ToInt32(lenBytes, 0) == 0)
            {
                return 0;
            }
                
            byte[] msgLength = ReadBytes(sizeof(int));
            return (msgLength == null) ? 0 : BitConverter.ToInt32(msgLength, 0);
        }

        /// <summary>
        /// Reads the first four bytes from the stream and decode
        /// it to get the message length in bytes
        /// </summary>
        /// <param name="token">The cancellation token</param>
        /// <returns>The incoming message's length in bytes</returns>
        private async Task<int> GetMessageLengthAsync(CancellationToken token)
        {
            byte[] lenBytes = await ReadBytesAsync(sizeof(int), token);
            if (lenBytes == null)
            {
                return 0;
            }
            if (BitConverter.IsLittleEndian)
            {
                Array.Reverse(lenBytes);
            }
            if (BitConverter.ToInt32(lenBytes, 0) == 0)
            {
                return 0;
            }
                
            byte[] msgLength = ReadBytes(sizeof(int));
            return (msgLength == null) ? 0 : BitConverter.ToInt32(msgLength, 0);
        }

        /// <summary>
        /// Copies the entire source buffer into the destination buffer the specified
        /// destination offset.
        /// </summary>
        /// <param name="source">The source buffer to be copied</param>
        /// <param name="dest">The destination buffer to copy to</param>
        /// <param name="destOffset">The offset at the destination buffer to begin
        /// copying.</param>
        /// <returns>The number of bytes copied</returns>
        private int CopyBytes(byte[] source, byte[] dest, int destOffset)
        {
            Buffer.BlockCopy(source, 0, dest, destOffset, source.Length);
            return source.Length;
        }
    }
}