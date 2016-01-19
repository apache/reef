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
using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.IO;
using Org.Apache.REEF.Utilities;
using ProtoBuf;

[module: SuppressMessage("StyleCop.CSharp.MaintainabilityRules", "SA1403:FileMayOnlyContainASingleNamespace", Justification = "Serializers for all protobuf types")]
[module: SuppressMessage("StyleCop.CSharp.MaintainabilityRules", "SA1402:FileMayOnlyContainASingleClass", Justification = "Serializers for all protobuf types")]

namespace Org.Apache.REEF.Common.Protobuf.ReefProtocol
{
    /// <summary>
    /// Add serializer/deserializer to REEFMessage
    /// </summary>
    public partial class REEFMessage
    {
        public REEFMessage(EvaluatorHeartbeatProto evaluatorHeartbeatProto)
        {
            _evaluatorHeartBeat = evaluatorHeartbeatProto;
        }

        public static REEFMessage Deserialize(byte[] bytes)
        {
            REEFMessage pbuf = null;
            using (var s = new MemoryStream(bytes))
            {
                pbuf = Serializer.Deserialize<REEFMessage>(s);
            }
            return pbuf;
        }

        public byte[] Serialize()
        {
            byte[] b = null;
            using (var s = new MemoryStream())
            {
                Serializer.Serialize<REEFMessage>(s, this);
                b = new byte[s.Position];
                var fullBuffer = s.GetBuffer();
                Array.Copy(fullBuffer, b, b.Length);
            }
            return b;
        }
    }

    /// <summary>
    /// Add serializer/deserializer to EvaluatorHeartbeatProto
    /// </summary>
    public partial class EvaluatorHeartbeatProto
    {
        public static EvaluatorHeartbeatProto Deserialize(byte[] bytes)
        {
            EvaluatorHeartbeatProto pbuf = null;
            using (var s = new MemoryStream(bytes))
            {
                pbuf = Serializer.Deserialize<EvaluatorHeartbeatProto>(s);
            }
            return pbuf;
        }

        public byte[] Serialize()
        {
            byte[] b = null;
            using (var s = new MemoryStream())
            {
                Serializer.Serialize<EvaluatorHeartbeatProto>(s, this);
                b = new byte[s.Position];
                var fullBuffer = s.GetBuffer();
                Array.Copy(fullBuffer, b, b.Length);
            }
            return b;
        }

        public override string ToString()
        {
            string contextStatus = string.Empty;
            string taskStatusMessage = string.Empty;
            foreach (ContextStatusProto contextStatusProto in context_status)
            {
                contextStatus += string.Format(CultureInfo.InvariantCulture, "evaluator {0} has context {1} in state {2} with recovery flag {3}",
                    evaluator_status.evaluator_id,
                    contextStatusProto.context_id,
                    contextStatusProto.context_state,
                    contextStatusProto.recovery);
            }
            if (task_status != null && task_status.task_message != null && task_status.task_message.Count > 0)
            {
                foreach (TaskStatusProto.TaskMessageProto taskMessageProto in task_status.task_message)
                {
                    taskStatusMessage += ByteUtilities.ByteArraysToString(taskMessageProto.message);
                }
            }
            return string.Format(CultureInfo.InvariantCulture, "EvaluatorHeartbeatProto: task_id=[{0}], task_status=[{1}], task_message=[{2}], evaluator_status=[{3}], context_status=[{4}], timestamp=[{5}], recoveryFlag =[{6}]",
                task_status == null ? string.Empty : task_status.task_id,
                task_status == null ? string.Empty : task_status.state.ToString(),
                taskStatusMessage,
                evaluator_status.state.ToString(),
                contextStatus,
                timestamp,
                recovery);
        }
    }
}