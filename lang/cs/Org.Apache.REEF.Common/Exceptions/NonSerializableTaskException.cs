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

namespace Org.Apache.REEF.Common.Exceptions
{
    /// <summary>
    /// Encapsulates <see cref="Exception#ToString"/> for an Exception from a 
    /// REEF Task that was not Serializable to the Job Driver.
    /// </summary>
    [Serializable]
    public sealed class NonSerializableTaskException : Exception
    {
        internal static NonSerializableTaskException UnableToSerialize(Exception originalException, SerializationException serializationException)
        {
            return new NonSerializableTaskException(originalException, serializationException);
        }

        internal static NonSerializableTaskException UnableToDeserialize(string exceptionString, SerializationException serializationException)
        {
            return new NonSerializableTaskException(exceptionString, serializationException);
        }

        private NonSerializableTaskException(Exception originalException, SerializationException serializationException)
            : base(GetNonSerializableExceptionMessage(originalException), serializationException)
        {
        }

        private NonSerializableTaskException(string exceptionString, SerializationException serializationException)
            : base(exceptionString, serializationException)
        {
        }

        private NonSerializableTaskException(SerializationInfo info, StreamingContext context)
            : base(info, context)
        {
        }

        private static string GetNonSerializableExceptionMessage(Exception e)
        {
            return string.Format("Unable to serialize the original Task Exception. Original Exception.ToString(): {0}", e);
        }
    }
}