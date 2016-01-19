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
using Org.Apache.REEF.Driver.Context;
using Org.Apache.REEF.Utilities;

namespace Org.Apache.REEF.Driver.Task
{
    /// <summary>
    /// Represents a running Task
    /// </summary>
    public interface IRunningTask : IIdentifiable, IDisposable
    {
        /// <summary>
        /// the context the task is running on.
        /// </summary>
        IActiveContext ActiveContext { get; }

        /// <summary>
        /// Sends the message
        /// </summary>
        /// <param name="message"></param>
        void Send(byte[] message);

        /// <summary>
        ///  Signal the task to suspend.
        /// </summary>
        /// <param name="message">a message that is sent to the Task.</param>
        void Suspend(byte[] message);

        /// <summary>
        /// Sends the message to the running task.
        /// </summary>
        void Suspend();

        /// <summary>
        /// Signal the task to shut down.
        /// </summary>
        /// <param name="message">a message that is sent to the Task.</param>
        void Dispose(byte[] message);
    }
}
