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
using Org.Apache.REEF.Bridge.Core.Tests.Fail.ThreadInterruptedException;
using Org.Apache.REEF.Common.Context;
using Org.Apache.REEF.Common.Tasks;
using Org.Apache.REEF.Driver;
using Org.Apache.REEF.Driver.Context;
using Org.Apache.REEF.Driver.Evaluator;
using Org.Apache.REEF.Driver.Task;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Exceptions;
using Org.Apache.REEF.Tang.Formats;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Tang.Util;
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.Bridge.Core.Tests.Fail.Task
{

    internal sealed class Driver :
        IObserver<IAllocatedEvaluator>,
        IObserver<IRunningTask>,
        IObserver<IActiveContext>,
        IObserver<IDriverStarted>
    {
        private static readonly Logger Log = Logger.GetLogger(typeof(Driver));

        [NamedParameter("the fail task to test")]
        internal sealed class FailTaskName : Name<string>
        {
        }

        private readonly string _failTaskName;

        private readonly IEvaluatorRequestor _evaluatorRequestor;

        private string _taskId;

        [Inject]
        private Driver(IEvaluatorRequestor evaluatorRequestor,
            [Parameter(Value = typeof(FailTaskName))]
            string failTaskName)
        {
            _failTaskName = failTaskName;
            _evaluatorRequestor = evaluatorRequestor;
        }

        public void OnError(System.Exception error)
        {
            throw new NotImplementedException();
        }

        public void OnCompleted()
        {
            throw new NotImplementedException();
        }

        public void OnNext(IAllocatedEvaluator eval)
        {
            try
            {
                _taskId = _failTaskName + "_" + eval.Id;
                Log.Log(Level.Info, "Submit task: {0}", _taskId);

                IConfiguration contextConfig =
                    ContextConfiguration.ConfigurationModule.Set(ContextConfiguration.Identifier, _taskId).Build();

                ConfigurationModule taskConfig =
                    TaskConfiguration.ConfigurationModule.Set(TaskConfiguration.Identifier, _taskId);

                switch (_failTaskName)
                {
                    case "FailTask":
                        taskConfig = taskConfig.Set(TaskConfiguration.Task, GenericType<FailTask>.Class);
                        break;
                    case "FailTaskCall":
                        taskConfig = taskConfig.Set(TaskConfiguration.Task, GenericType<FailTaskCall>.Class);
                        break;
                    case "FailTaskMsg":
                        taskConfig = taskConfig
                            .Set(TaskConfiguration.Task, GenericType<FailTaskMsg>.Class)
                            .Set(TaskConfiguration.OnMessage, GenericType<FailTaskMsg>.Class);
                        break;
                    case "FailTaskSuspend":
                        taskConfig = taskConfig
                            .Set(TaskConfiguration.Task, GenericType<FailTaskSuspend>.Class)
                            .Set(TaskConfiguration.OnSuspend, GenericType<FailTaskSuspend>.Class);
                        break;
                    case "FailTaskStart":
                        taskConfig = taskConfig
                            .Set(TaskConfiguration.Task, GenericType<FailTaskStart>.Class)
                            .Set(TaskConfiguration.OnTaskStart, GenericType<FailTaskStart>.Class);
                        break;
                    case "FailTaskStop":
                        taskConfig = taskConfig
                            .Set(TaskConfiguration.Task, GenericType<FailTaskStop>.Class)
                            .Set(TaskConfiguration.OnTaskStop, GenericType<FailTaskStop>.Class)
                            .Set(TaskConfiguration.OnClose, GenericType<FailTaskStop>.Class);
                        break;
                    case "FailTaskClose":
                        taskConfig = taskConfig
                            .Set(TaskConfiguration.Task, GenericType<FailTaskClose>.Class)
                            .Set(TaskConfiguration.OnClose, GenericType<FailTaskClose>.Class);
                        break;
                    default:
                        break;
                }

                eval.SubmitContextAndTask(contextConfig, taskConfig.Build());
            }
            catch (BindException ex)
            {
                Log.Log(Level.Warning, "Configuration error", ex);
                throw new DriverSideFailure("Configuration error", ex);
            }
        }

        public void OnNext(IRunningTask task)
        {
            Log.Log(Level.Info,
                "TaskRuntime: {0} expect {1}",
                task.Id, _taskId);

            if (!_taskId.Equals(task.Id))
            {
                throw new DriverSideFailure($"Task ID {task.Id} not equal expected ID {_taskId}");
            }

            switch (_failTaskName)
            {
                case "FailTaskMsg":
                    Log.Log(Level.Info, "TaskRuntime: Send message: {0}", task);
                    task.Send(new byte[0]);
                    break;
                case "FailTaskSuspend":
                    Log.Log(Level.Info, "TaskRuntime: Suspend: {0}", task);
                    task.Suspend();
                    break;
                case "FailTaskStop":
                case "FailTaskClose":
                    Log.Log(Level.Info, "TaskRuntime: Stop/Close: {0}", task);
                    task.Dispose();
                    break;
                default:
                    break;
            }
        }

        public void OnNext(IActiveContext context)
        {
            throw new DriverSideFailure($"Unexpected ActiveContext message: {context.Id}");
        }

        public void OnNext(IDriverStarted start)
        {
            Log.Log(Level.Info, "StartTime: {0}", start);
            _evaluatorRequestor.Submit(_evaluatorRequestor.NewBuilder()
                .SetNumber(1).SetMegabytes(128).SetNumber(1).Build());
        }
    }
}
