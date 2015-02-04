/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

using Org.Apache.Reef.Common.Task;
using Org.Apache.Reef.Utilities;
using System;

namespace Org.Apache.Reef.Common.Exceptions
{
    public class EvaluatorException : System.Exception, IIdentifiable
    {
        private readonly string _evaluatorId;

        public EvaluatorException(string evaluatorId)
        {
            _evaluatorId = evaluatorId;
            RunningTask = null;
        }

        public EvaluatorException(string evaluatorId, string message, System.Exception cause)
            : base(message, cause)
        {
            _evaluatorId = evaluatorId;
            RunningTask = null;
        }

        public EvaluatorException(string evaluatorId, string message)
            : this(evaluatorId, message, (IRunningTask)null)
        {
        }

        public EvaluatorException(string evaluatorId, string message, IRunningTask runningTask)
            : base(message)
        {
            _evaluatorId = evaluatorId;
            RunningTask = runningTask;
        }

        public EvaluatorException(string evaluatorId, System.Exception cause)
            : this(evaluatorId, cause, null)
        {
        }

        public EvaluatorException(string evaluatorId, Exception cause, IRunningTask runningTask)
            : base(string.Empty, cause)
        {
            _evaluatorId = evaluatorId;
            RunningTask = runningTask;
        }

        public IRunningTask RunningTask { get; set; }

        public string Id
        {
            get { return _evaluatorId; }
            set { }
        }
    }
}
