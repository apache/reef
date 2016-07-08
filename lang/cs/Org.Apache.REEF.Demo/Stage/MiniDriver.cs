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

using System.Collections.Generic;
using Org.Apache.REEF.Driver.Context;
using Org.Apache.REEF.Driver.Evaluator;
using Org.Apache.REEF.Driver.Task;

namespace Org.Apache.REEF.Demo.Stage
{
    public abstract class MiniDriver
    {
        public abstract void OnStart(IDictionary<IActiveContext, ISet<BlockInfo>> blockDistribution);

        public virtual void OnRunningTask(IRunningTask runningTask)
        {
        }

        public virtual void OnFailedTask(IFailedTask failedTask)
        {
        }

        public virtual void OnFailedContext(IFailedContext failedContext)
        {
        }

        public virtual void OnFailedEvaluator(IFailedEvaluator failedEvaluator)
        {
        }
    }
}