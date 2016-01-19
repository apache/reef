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

using Org.Apache.REEF.Wake.Remote;

namespace Org.Apache.REEF.Common.Protobuf.ReefProtocol
{
    public class EvaluatorHeartbeatProtoCodec : ICodec<EvaluatorHeartbeatProto>
    {
        public byte[] Encode(EvaluatorHeartbeatProto obj)
        {
            EvaluatorHeartbeatProto pbuf = new EvaluatorHeartbeatProto();

            pbuf.evaluator_status = obj.evaluator_status;
            return pbuf.Serialize();
        }

        public EvaluatorHeartbeatProto Decode(byte[] data)
        {
            EvaluatorHeartbeatProto pbuf = EvaluatorHeartbeatProto.Deserialize(data);
            return pbuf;
        }
    }
}
