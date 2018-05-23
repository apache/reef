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
using System.Runtime.Serialization;
using Org.Apache.REEF.Common.Client;
using Org.Apache.REEF.Common.Exceptions;
using Org.Apache.REEF.Utilities;

namespace Org.Apache.REEF.Bridge.Core.Common.Client.Events
{
    internal sealed class FailedJob : IFailedJob
    {
        public FailedJob(string jobId, string message, byte[] data = null)
        {
            Id = jobId;
            Message = message;
            Data = Optional<byte[]>.OfNullable(data);
        }

        public string Id { get; }
        public string Message { get; set; }
        public Optional<string> Description { get; set; }
        public Optional<string> Reason { get; set; }
        public Optional<byte[]> Data { get; set; }
        public Exception AsError()
        {
            if (Data.IsPresent())
            {
                Exception inner;
                try
                {
                    inner = (Exception) ByteUtilities.DeserializeFromBinaryFormat(Data.Value);
                }
                catch (SerializationException se)
                {
                    inner = NonSerializableEvaluatorException.UnableToDeserialize(
                        "Not able to deserialize job exception.",
                        se);
                }
                return new JobException(Id, Message, inner);
            }
            else
            {
                return new JobException(Id, Message);
            }
        }
    }
}
