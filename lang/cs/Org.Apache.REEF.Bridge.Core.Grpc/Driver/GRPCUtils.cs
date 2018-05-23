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
using Google.Protobuf;
using Org.Apache.REEF.Bridge.Core.Proto;
using Org.Apache.REEF.Common.Exceptions;
using Org.Apache.REEF.Utilities;
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.Bridge.Core.Grpc.Driver
{
    internal static class GrpcUtils
    {
        private static readonly Logger Log = Logger.GetLogger(typeof(GrpcUtils));

        public static ExceptionInfo SerializeException(Exception ex)
        {
            ByteString serializedException;
            try
            {
                serializedException = ByteString.CopyFrom(ByteUtilities.SerializeToBinaryFormat(ex));
            }
            catch (SerializationException se)
            {
                Log.Log(Level.Warning, "Unable to serialize exception", ex);
                serializedException = ByteString.CopyFrom(ByteUtilities.SerializeToBinaryFormat(
                    NonSerializableJobException.UnableToSerialize(ex, se)));
            }
            return new ExceptionInfo()
            {
                NoError = false,
                Name = ex.ToString(),
                Message = ex.Message,
                Data = serializedException
            };
        }
    }
}
