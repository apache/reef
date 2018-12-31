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

using Org.Apache.REEF.Utilities.Attributes;
using System;
using System.Runtime.Serialization;

namespace Org.Apache.REEF.Network.Elastic.Failures
{
    /// <summary>
    /// A serializable exception that represents a task operator error.
    /// </summary>
    [Serializable]
    [Unstable("0.16", "API may change")]
    public class OperatorException : Exception, ISerializable
    {
        public readonly int _id;
        public readonly string _additionalInfo;

        /// <summary>
        /// Constructor. A serializable exception object that represents a task operator error.
        /// All the operator related errors should be captured in this type of exception in order
        /// to be proprierly handled by the elastic framework.
        /// <param name="message">The exception message</param>
        /// <param name="id">The id of the operator where the exception is triggered</param>
        /// </summary>
        public OperatorException(string message, int id)
            : base(GetMessagePrefix(id) + message)
        {
            _id = id;
        }

        /// <summary>
        /// Constructor. A serializable exception object that represents a task operator error and wraps an inner exception.
        /// </summary>
        /// <param name="message">The exception message</param>
        /// <param name="id">The id of the operator where the exception is triggered</param>
        /// <param name="innerException">Inner exception</param>
        public OperatorException(string message, int id, Exception innerException)
            : base(GetMessagePrefix(id) + message, innerException)
        {
            _id = id;
        }

        /// <summary>
        /// Constructor. A serializable exception object that represents a task operator error and wraps an inner exception
        /// plus some additional operator specific information.
        /// </summary>
        /// <param name="message">The exception message</param>
        /// <param name="id">The id of the operator where the exception is triggered</param>
        /// <param name="innerException">Inner exception</param>
        /// <param name="info">Additional operator speicifc information on the failure</param>
        public OperatorException(string message, int id, Exception innerException, string info)
            : base(GetMessagePrefix(id) + message, innerException)
        {
            _id = id;
            _additionalInfo = info;
        }

        /// <summary>
        /// Constructor that generate an operator exception from a serialized buffer.
        /// </summary>
        /// <param name="info">The buffer containing the exception information</param>
        /// <param name="context">The streaming context</param>
        public OperatorException(SerializationInfo info, StreamingContext context)
            : base(info, context)
        {
            _id = info.GetInt32("id");
            _additionalInfo = info.GetString("info");
        }

        /// <summary>
        /// The identifier of the operator throwing the exception.
        /// </summary>
        public int OperatorId
        {
            get { return _id; }
        }

        /// <summary>
        /// Some additional info for the exception.
        /// </summary>
        public string AdditionalInfo
        {
            get { return _additionalInfo; }
        }

        /// <summary>
        /// Serialize the exception.
        /// </summary>
        /// <param name="info">The buffer where to add the exception information</param>
        /// <param name="context">The streaming context</param>
        public new void GetObjectData(SerializationInfo info, StreamingContext context)
        {
            base.GetObjectData(info, context);
            info.AddValue("id", _id, typeof(int));
            info.AddValue("info", _additionalInfo, typeof(string));
        }

        private static string GetMessagePrefix(int id)
        {
            return "Operator " + id + " : ";
        }
    }
}