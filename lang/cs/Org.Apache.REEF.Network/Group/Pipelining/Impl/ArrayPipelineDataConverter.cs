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

using Org.Apache.REEF.Network.Group.Config;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Utilities.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Serialization;

namespace Org.Apache.REEF.Network.Group.Pipelining.Impl
{
    /// <summary>
    /// An implementation of IPipelineDataConverter for pipelining Arrays of objects.
    /// </summary>
    /// <typeparam name="T">The type of object of the array to pipeline</typeparam>
    public sealed class ArrayPipelineDataConverter<T> : IPipelineDataConverter<T[]> where T : new()
    {
        private readonly int _pipelineMessageSize;
        private static readonly Logger Logger = Logger.GetLogger(typeof(ArrayPipelineDataConverter<T>));

        [Inject]
        private ArrayPipelineDataConverter([Parameter(typeof(GroupCommConfigurationOptions.PipelineMessageSize))] int pipelineMessageSize)
        {
            if (pipelineMessageSize <= 0)
            {
                throw new ArgumentException("PipelinePackageSize must be strictly positive");
            }
            _pipelineMessageSize = pipelineMessageSize;
        }

        /// <summary>
        /// Converts the original message to be communicated in to multiple messages, breaking the array into pieces of size PipelineMessageSize
        /// </summary>
        /// <param name="message">The original message</param>
        /// <returns>The list of pipelined messages</returns>
        public List<PipelineMessage<T[]>> PipelineMessage(T[] message)
        {
            int messageCount = ((message.Length - 1) / _pipelineMessageSize) + 1;
            List<PipelineMessage<T[]>> messageList = new List<PipelineMessage<T[]>>(messageCount);
            int offset = 0;
            while (offset < message.Length)
            {
                int subLen = Math.Min(message.Length - offset, _pipelineMessageSize);
                if (subLen <= 0)
                {
                    throw new ArithmeticException("Tried to create a pipeline package with fewer than 1 element.");
                }
                T[] data = new T[subLen];
                Array.Copy(message, offset, data, 0, subLen);
                bool isLast = subLen + offset == message.Length;
                messageList.Add(new PipelineMessage<T[]>(data, isLast));
                offset += subLen;
            }
            if (messageCount != messageList.Count)
            {
                throw new SerializationException(string.Format("The wrong number of pipeline packages were created: Expected {0} but created {1}.", messageCount, messageList.Count));
            }

            return messageList;
        }

        /// <summary>
        /// Constructs the full final message from the communicated pipelined message
        /// </summary>
        /// <param name="pipelineMessage">A list of received pipelined messages</param>
        /// <returns>The full constructed message</returns>
        public T[] FullMessage(List<PipelineMessage<T[]>> pipelineMessage)
        {
            if (pipelineMessage.Count == 0)
            {
                return null;
            }
            if (pipelineMessage.Count == 1)
            {
                return pipelineMessage[0].Data;
            }

            int nElements = pipelineMessage.Sum(v => v.Data.Length);
            if (nElements == 0)
            {
                return null;
            }

            int offset = 0;
            T[] values = new T[nElements];
            foreach (var message in pipelineMessage)
            {
                Array.Copy(message.Data, 0, values, offset, message.Data.Length);
                offset += message.Data.Length;
            }
            if (offset != nElements)
            {
                throw new SerializationException(string.Format("The pipeline packages were deserialized incorrectly created: Expected {0} but created {1}.", nElements, offset));
            }

            return values;
        }
    }
}
