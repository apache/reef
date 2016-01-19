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
using Org.Apache.REEF.Driver.Task;
using Org.Apache.REEF.Utilities;

namespace Org.Apache.REEF.Driver.Evaluator
{
    //// Public only such that it can be used in the bridge.

    /// <summary>
    /// </summary>
    public sealed class EvaluatorException : Exception, IIdentifiable
    {
        private readonly string _evaluatorId;
        private readonly IRunningTask _runningTask;

        internal EvaluatorException(string evaluatorId, IRunningTask runningTask = null)
        {
            _evaluatorId = evaluatorId;
            _runningTask = runningTask;
        }

        internal EvaluatorException(string evaluatorId, string message, Exception cause, IRunningTask runningTask = null)
            : base(message, cause)
        {
            _evaluatorId = evaluatorId;
            _runningTask = runningTask;
        }

        internal EvaluatorException(string evaluatorId, string message, IRunningTask runningTask = null)
            : base(message)
        {
            _evaluatorId = evaluatorId;
            _runningTask = runningTask;
        }

        internal EvaluatorException(string evaluatorId, Exception cause, IRunningTask runningTask = null)
            : base(string.Empty, cause)
        {
            _evaluatorId = evaluatorId;
            _runningTask = runningTask;
        }

        /// <summary>
        /// The task that was running on the Evaluator or null if none was running exists.
        /// </summary>
        public IRunningTask RunningTask
        {
            get { return _runningTask; }
        }

        public string Id
        {
            get { return _evaluatorId; }
        }
    }
}