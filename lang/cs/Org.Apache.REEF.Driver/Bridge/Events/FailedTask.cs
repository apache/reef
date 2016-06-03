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
using Org.Apache.REEF.Driver.Bridge.Clr2java;
using Org.Apache.REEF.Driver.Context;
using Org.Apache.REEF.Driver.Task;
using Org.Apache.REEF.Utilities;
using Org.Apache.REEF.Utilities.Diagnostics;
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.Driver.Bridge.Events
{
    internal sealed class FailedTask : IFailedTask
    {
        private static readonly Logger Logger = Logger.GetLogger(typeof(FailedTask));
        
        private readonly Exception _cause;
        
        public FailedTask(IFailedTaskClr2Java failedTaskClr2Java)
        {
            var serializedInfo = failedTaskClr2Java.GetFailedTaskSerializedAvro();
            var avroFailedTask = AvroJsonSerializer<AvroFailedTask>.FromBytes(serializedInfo);

            Id = avroFailedTask.identifier;

            // Data is simply the serialized Exception.ToString.
            Data = Optional<byte[]>.OfNullable(avroFailedTask.data);

            // Message can be overwritten in Java, if the C# Message is null and the Task failure is caused by an Evaluator failure.
            Message = string.IsNullOrWhiteSpace(avroFailedTask.message) ? "No message in Failed Task." : avroFailedTask.message;

            // Gets the Exception.
            _cause = GetCause(avroFailedTask.cause, ByteUtilities.ByteArraysToString(avroFailedTask.data));

            // This is always empty, even in Java.
            Description = Optional<string>.Empty();

            ActiveContextClr2Java = failedTaskClr2Java.GetActiveContext();
        }

        public Optional<string> Reason { get; set; }

        public string Id { get; private set; }

        public string Message { get; set; }

        public Optional<string> Description { get; set; }

        public Optional<byte[]> Data { get; set; }

        [DataMember]
        private IActiveContextClr2Java ActiveContextClr2Java { get; set; }

        /// <summary>
        /// Access the context the task ran (and crashed) on, if it could be recovered.
        /// An ActiveContext is given when the task fails but the context remains alive.
        /// On context failure, the context also fails and is surfaced via the FailedContext event.
        /// Note that receiving an ActiveContext here is no guarantee that the context (and evaluator)
        /// are in a consistent state. Application developers need to investigate the reason available
        /// via getCause() to make that call.
        /// return the context the Task ran on.
        /// </summary>
        public Optional<IActiveContext> GetActiveContext()
        {
            if (ActiveContextClr2Java == null)
            {
                return Optional<IActiveContext>.Empty();
            }

            return Optional<IActiveContext>.Of(new ActiveContext(ActiveContextClr2Java));
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

        private static Exception GetCause(
            byte[] serializedCause, string originalTaskExceptionToString)
        {
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
                Exceptions.Caught(se, Level.Info,
                    "Exception from Task was not able to be deserialized, returning a NonSerializableTaskException.", Logger);

                return NonSerializableTaskException.UnableToDeserialize(originalTaskExceptionToString, se);
            }
        }
    }
}
