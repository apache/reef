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
using Org.Apache.REEF.Common.Avro;
using Org.Apache.REEF.Common.Exceptions;
using Org.Apache.REEF.Common.Runtime.Evaluator.Task;
using Org.Apache.REEF.Driver.Context;
using Org.Apache.REEF.Driver.Task;
using Org.Apache.REEF.Utilities;
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.Bridge.Core.Common.Driver.Events
{
    internal sealed class BridgeFailedTask : IFailedTask
    {
        private static readonly Logger Log = Logger.GetLogger(typeof(BridgeFailedTask));

        private readonly Optional<IActiveContext> _activeContext;

        private Exception _cause = null;

        public BridgeFailedTask(Optional<IActiveContext> activeContext, string id, string message, byte[] data) :
            this(activeContext, id, message, null, null, data)
        {
        }

        public BridgeFailedTask(
            Optional<IActiveContext> activeContext, 
            string id, 
            string message, 
            string description, 
            string reason, 
            byte[] data)
        {
            _activeContext = activeContext;
            Id = id;
            Message = message;
            Description = Optional<string>.OfNullable(description);
            Reason = Optional<string>.OfNullable(reason);
            if (data != null)
            {
                var avroFailedTask = AvroJsonSerializer<AvroFailedTask>.FromBytes(data);

                if (!id.Equals(avroFailedTask.identifier))
                {
                    Log.Log(Level.Error, "Task error id {0} does not match task id {1}", 
                        new object[] {avroFailedTask.identifier, id});
                }

                // Data is simply the serialized Exception.ToString.
                Data = Optional<byte[]>.OfNullable(avroFailedTask.data);

                // Message can be overwritten in Java, if the C# Message is null and the Task failure is caused by an Evaluator failure.
                Message = string.IsNullOrWhiteSpace(avroFailedTask.message)
                    ? "No message in Failed Task."
                    : avroFailedTask.message;

                // Gets the Exception.
                _cause = GetCause(avroFailedTask.cause, ByteUtilities.ByteArraysToString(avroFailedTask.data));

                // This is always empty, even in Java.
                Description = Optional<string>.Empty();
            }
            else
            {
                Log.Log(Level.Warning, "no exception data");
                Data = Optional<byte[]>.Empty();
            }
        }

        public string Id { get; }
        public string Message { get; set; }
        public Optional<string> Description { get; set; }
        public Optional<string> Reason { get; set; }
        public Optional<byte[]> Data { get; set; }

        public Optional<IActiveContext> GetActiveContext()
        {
            return _activeContext;
        }

        /// <summary>
        /// Returns the Exception causing the Failed Task.
        /// </summary>
        /// <returns>the Exception causing the Failed Task.</returns>
        /// <remarks>
        /// If the Exception was caused by a control flow error (start, stop, suspend), 
        /// a <see cref="TaskClientCodeException"/> is expected.
        /// If the original Exception was not serializable, a <see cref="NonSerializableTaskException"/> is expected.
        /// If the Exception was missing, presumably caused by a failed Evaluator, a 
        /// <see cref="TaskExceptionMissingException"/> is expected.
        /// </remarks>
        public Exception AsError()
        {
            return _cause;
        }

        private static Exception GetCause(byte[] serializedCause, string originalTaskExceptionToString)
        {
            // TODO[JIRA REEF-1422]: Distinguish between Java Task Exception and missing Exception.
            if (ByteUtilities.IsNullOrEmpty(serializedCause))
            {
                return new TaskExceptionMissingException(
                    "Task failed without an Exception, presumably caused by an Exception failure. Please inspect the FailedTask message.");
            }
            try
            {
                return (Exception)ByteUtilities.DeserializeFromBinaryFormat(serializedCause);
            }
            catch (SerializationException se)
            {
                return NonSerializableTaskException.UnableToDeserialize(originalTaskExceptionToString, se);
            }
        }
    }
}
