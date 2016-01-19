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
using System.Globalization;
using Org.Apache.REEF.Utilities;
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.Common.Api
{
    public abstract class AbstractFailure : IFailure
    {
        private static readonly Logger LOGGER = Logger.GetLogger(typeof(AbstractFailure));

        /// <summary>
        /// Most detailed error message constructor that takes all parameters possible.
        /// </summary>
        /// <param name="id">Identifier of the entity that produced the error. Cannot be null.</param>
        /// <param name="message">One-line error message. Cannot be null.</param>
        /// <param name="description">Long error description. Can be null.</param>
        /// <param name="cause">Exception that caused the error. Can be null.</param>
        /// <param name="data">byte array that contains serialized version of the error. Can be null.</param>
        protected AbstractFailure(string id, string message, string description, Exception cause, byte[] data)
        {
            if (string.IsNullOrEmpty(id))
            {
                Org.Apache.REEF.Utilities.Diagnostics.Exceptions.Throw(new ArgumentException("id"), LOGGER);
            }
            if (string.IsNullOrEmpty(message))
            {
                Org.Apache.REEF.Utilities.Diagnostics.Exceptions.Throw(new ArgumentException("message"), LOGGER);
            }
            Id = id;
            Message = message;
            Description = Optional<string>.OfNullable(string.IsNullOrEmpty(description) ? GetStackTrace(cause) : description);
            Cause = Optional<Exception>.OfNullable(cause);
            Data = Optional<byte[]>.OfNullable(data);
        }

        /// <summary>
        ///  Build error message given the entity ID and the short error message.
        /// </summary>
        /// <param name="id"></param>
        /// <param name="message"></param>
        protected AbstractFailure(string id, string message)
            : this(id, message, null, null, null)
        {
        }

        /// <summary>
        ///  Build error message given the failed entity ID and  Exception.
        ///  Populates the message with the Exception.getMessage() result, and stores
        ///  the exception stack trace in the description.
        /// </summary>
        /// <param name="id"></param>
        /// <param name="cause"></param>
        protected AbstractFailure(string id, Exception cause)
        {
            if (string.IsNullOrEmpty(id))
            {
                Org.Apache.REEF.Utilities.Diagnostics.Exceptions.Throw(new ArgumentException("id"), LOGGER);
            }
            Id = id;
            Message = cause.Message;
            Description = Optional<string>.Of(GetStackTrace(cause));
            Cause = Optional<Exception>.Of(cause);
            Data = Optional<byte[]>.Empty();
        }

        /// <summary>
        /// Build error message given the entity ID plus short and long error message.
        /// </summary>
        /// <param name="id"></param>
        /// <param name="message"></param>
        /// <param name="description"></param>
        protected AbstractFailure(string id, string message, string description)
            : this(id, message, description, null, null)
        {
        }

        /// <summary>
        /// Identifier of the entity that produced the error. Cannot be null.
        /// </summary>
        public string Id { get; private set; }

        public string Message { get; set; }

        public Optional<string> Description { get; set; }

        public Optional<string> Reason { get; set; }

        public Optional<Exception> Cause { get; set; }

        public Optional<byte[]> Data { get; set; }

        public Exception AsError()
        {
            return Cause.IsPresent() ? Cause.Value : new InvalidOperationException(ToString());
        }

        /// <summary>
        ///  Helper function: produce the string that contains the given exception's stack trace. Returns null if the argument is null.
        /// </summary>
        /// <param name="ex"></param>
        public string GetStackTrace(Exception ex)
        {
            if (ex == null)
            {
                return null;
            }
            else
            {
                return ex.StackTrace;
            }
        }

        public override string ToString()
        {
            return string.Format(CultureInfo.InvariantCulture, "{0} with id={1} failed: {2}", GetType(), Id, Message);
        }
    }
}
