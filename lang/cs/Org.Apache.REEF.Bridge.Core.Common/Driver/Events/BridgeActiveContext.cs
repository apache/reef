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

using Org.Apache.REEF.Driver.Context;
using Org.Apache.REEF.Driver.Evaluator;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Utilities;
using System;

namespace Org.Apache.REEF.Bridge.Core.Common.Driver.Events
{
    internal sealed class BridgeActiveContext : IActiveContext
    {
        private readonly IDriverServiceClient _driverServiceClient;

        public string Id { get; }
        public string EvaluatorId { get; }
        public Optional<string> ParentId { get; }
        public IEvaluatorDescriptor EvaluatorDescriptor { get; }

        public BridgeActiveContext(
            IDriverServiceClient driverServiceClient,
            string id,
            string evaluatorId,
            Optional<string> parentId,
            IEvaluatorDescriptor evaluatorDescriptor)
        {
            _driverServiceClient = driverServiceClient;
            Id = id;
            EvaluatorId = evaluatorId;
            ParentId = parentId;
            EvaluatorDescriptor = evaluatorDescriptor;
        }

        public void Dispose()
        {
            _driverServiceClient.OnContextClose(Id);
        }

        public void SubmitTask(IConfiguration taskConf)
        {
            _driverServiceClient.OnContextSubmitTask(Id, taskConf);
        }

        public void SubmitContext(IConfiguration contextConfiguration)
        {
            _driverServiceClient.OnContextSubmitContext(Id, contextConfiguration);
        }

        public void SubmitContextAndService(IConfiguration contextConfiguration, IConfiguration serviceConfiguration)
        {
            throw new NotImplementedException();
        }

        public void SendMessage(byte[] message)
        {
            _driverServiceClient.OnContextMessage(Id, message);
        }
    }
}