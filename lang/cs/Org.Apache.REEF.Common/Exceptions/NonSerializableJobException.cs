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
using System.Runtime.Serialization;
using System.Text;

namespace Org.Apache.REEF.Common.Exceptions
{
    [Serializable]
    public sealed class NonSerializableJobException : Exception
    {

        public string[] StackTrace { get;  }

        internal static NonSerializableJobException UnableToSerialize(Exception originalException, SerializationException serializationException)
        {
            return new NonSerializableJobException(originalException, serializationException);
        }

        internal static NonSerializableJobException UnableToDeserialize(string exceptionString, SerializationException serializationException)
        {
            return new NonSerializableJobException(exceptionString, serializationException);
        }

        private NonSerializableJobException(Exception originalException, SerializationException serializationException)
            : base(GetNonSerializableExceptionMessage(originalException), serializationException)
        {
            StackTrace = GetTraceString(originalException);
        }

        private NonSerializableJobException(string exceptionString, SerializationException serializationException)
            : base(exceptionString, serializationException)
        {
            StackTrace = null;
        }

        private NonSerializableJobException(SerializationInfo info, StreamingContext context)
            : base(info, context)
        {
        }

        private static string[] GetTraceString(Exception ex)
        {
            var trace = new System.Diagnostics.StackTrace(ex, true);
            return trace.GetFrames().Select(t => t.ToString()).ToArray();
        }

        private static string GetNonSerializableExceptionMessage(Exception e)
        {
            return string.Format("Unable to serialize the original Evaluator Exception. Original Exception.ToString(): {0}", e);
        }
    }
}
