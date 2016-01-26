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
using Org.Apache.REEF.Common.Protobuf.ReefProtocol;
using Org.Apache.REEF.Common.Runtime.Evaluator.Context;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Wake.Time.Event;

namespace Org.Apache.REEF.Common.Runtime.Evaluator
{
    [DefaultImplementation(typeof(HeartBeatManager))]
    internal interface IHeartBeatManager : IObserver<Alarm>
    {
        /// <summary>
        /// Return EvaluatorRuntime referenced from HeartBeatManager
        /// </summary>
        EvaluatorRuntime EvaluatorRuntime { get; }

        /// <summary>
        /// Return ContextManager referenced from HeartBeatManager
        /// </summary>
        ContextManager ContextManager { get; }

        /// <summary>
        /// EvaluatorSettings contains the configuration data of the evaluators
        /// </summary>
        EvaluatorSettings EvaluatorSettings { get; }

        void Send(EvaluatorHeartbeatProto evaluatorHeartbeatProto);

        /// <summary>
        /// Assemble a complete new heartbeat and send it out.
        /// </summary>
        void OnNext();

        /// <summary>
        /// Called with a specific TaskStatus that must be delivered to the driver
        /// </summary>
        /// <param name="taskStatusProto"></param>
        void OnNext(TaskStatusProto taskStatusProto);

        /// <summary>
        /// Called with a specific ContextStatusProto that must be delivered to the driver
        /// </summary>
        /// <param name="contextStatusProto"></param>
        void OnNext(ContextStatusProto contextStatusProto);

        /// <summary>
        /// Called with a specific EvaluatorStatus that must be delivered to the driver
        /// </summary>
        /// <param name="evaluatorStatusProto"></param>
        void OnNext(EvaluatorStatusProto evaluatorStatusProto);
    }
}
