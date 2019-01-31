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
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Network.Elastic.Task;
using Org.Apache.REEF.Network.Elastic.Operators;
using Org.Apache.REEF.Network.Elastic.Operators.Physical;
using Org.Apache.REEF.Network.Elastic.Task.Default;
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.Network.Examples.Elastic
{
    public sealed class BroadcastMasterTask : DefaultElasticTask
    {
        private static readonly Logger LOGGER = Logger.GetLogger(typeof(BroadcastMasterTask));

        [Inject]
        private BroadcastMasterTask(CancellationSource source, IElasticContext context)
            : base(source, context, "Broadcast")
        {
        }

        protected override void Execute(byte[] memento, Workflow workflow)
        {
            var rand = new Random();
            int number = 0;

            foreach (var op in workflow)
            {
                number = rand.Next();

                switch (op.OperatorType)
                {
                    case OperatorType.Broadcast:
                        var sender = workflow.Current as IElasticBroadcast<int>;

                        sender.Send(number);

                        LOGGER.Log(Level.Info, "Master has sent {0}", number);
                        break;

                    default:
                        throw new InvalidOperationException(
                            $"Operation {workflow.Current} in workflow not implemented.");
                }
            }
        }
    }
}