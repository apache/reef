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
using Org.Apache.REEF.Driver;
using Org.Apache.REEF.Driver.Context;
using Org.Apache.REEF.Driver.Evaluator;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Implementations.Configuration;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Driver.Task;
using Org.Apache.REEF.Common.Context;
using Org.Apache.REEF.Utilities.Attributes;

namespace Org.Apache.REEF.Network.Elastic.Driver.Default
{
    /// <summary>
    /// Default implementation of the elastic driver.
    /// </summary>
    [Unstable("0.16", "API may change")]
    public abstract class DefaultElasticDriver :
        IObserver<IAllocatedEvaluator>,
        IObserver<IActiveContext>,
        IObserver<IDriverStarted>,
        IObserver<IRunningTask>,
        IObserver<ICompletedTask>,
        IObserver<IFailedEvaluator>,
        IObserver<IFailedTask>,
        IObserver<ITaskMessage>
    {
        [Inject]
        protected DefaultElasticDriver(IElasticContext context)
        {
            Context = context;
        }

        public IElasticContext Context { get; }

        public IElasticTaskSetManager TaskSetManager { get; set; }

        public void OnNext(IDriverStarted value)
        {
            Context.Start();
        }

        public void OnNext(IAllocatedEvaluator allocatedEvaluator)
        {
            if (TaskSetManager.TryGetNextTaskContextId(allocatedEvaluator, out string identifier))
            {
                IConfiguration contextConf = ContextConfiguration.ConfigurationModule
                    .Set(ContextConfiguration.Identifier, identifier)
                    .Build();
                IConfiguration serviceConf = Context.GetElasticServiceConfiguration();
                IConfiguration codecConf = TaskSetManager.GetCodecConfiguration();

                serviceConf = Configurations.Merge(serviceConf, codecConf);
                allocatedEvaluator.SubmitContextAndService(contextConf, serviceConf);
            }
            else
            {
                allocatedEvaluator.Dispose();
            }
        }

        public void OnNext(IActiveContext activeContext)
        {
            TaskSetManager.OnNewActiveContext(activeContext);
        }

        public void OnNext(IRunningTask value)
        {
            TaskSetManager.OnTaskRunning(value);
        }

        public void OnNext(ICompletedTask value)
        {
            TaskSetManager.OnTaskCompleted(value);

            if (TaskSetManager.IsCompleted())
            {
                TaskSetManager.Dispose();
            }
        }

        public void OnNext(IFailedEvaluator failedEvaluator)
        {
            TaskSetManager.OnEvaluatorFailure(failedEvaluator);

            if (TaskSetManager.IsCompleted())
            {
                TaskSetManager.Dispose();
            }
        }

        public void OnNext(IFailedTask failedTask)
        {
            TaskSetManager.OnTaskFailure(failedTask);

            if (TaskSetManager.IsCompleted())
            {
                TaskSetManager.Dispose();
            }
        }

        public void OnNext(ITaskMessage taskMessage)
        {
            TaskSetManager.OnTaskMessage(taskMessage);
        }

        public void OnCompleted()
        {
            TaskSetManager.Dispose();
        }

        public void OnError(Exception error)
        {
            TaskSetManager.Dispose();
        }
    }
}