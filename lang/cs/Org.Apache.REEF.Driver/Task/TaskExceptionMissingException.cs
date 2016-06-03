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

namespace Org.Apache.REEF.Driver.Task
{
    /// <summary>
    /// An Exception that is used to indicate that there are no Task Exceptions
    /// in a Context. Could occur when an Evaluator fails or when a Context fails
    /// before a Task is reported to be initialized.
    /// </summary>
    [Serializable]
    public sealed class TaskExceptionMissingException : Exception
    {
        internal TaskExceptionMissingException(string message) : base(message)
        {
        }

        private TaskExceptionMissingException(SerializationInfo serializationInfo, StreamingContext streamingContext)
            : base(serializationInfo, streamingContext)
        {
        }
    }
}