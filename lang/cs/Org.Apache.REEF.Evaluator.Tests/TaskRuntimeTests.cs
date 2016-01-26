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
using System.Threading;
using NSubstitute;
using Org.Apache.REEF.Common.Context;
using Org.Apache.REEF.Common.Protobuf.ReefProtocol;
using Org.Apache.REEF.Common.Runtime.Evaluator;
using Org.Apache.REEF.Common.Runtime.Evaluator.Task;
using Org.Apache.REEF.Common.Tasks;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Tang.Util;
using Xunit;

namespace Org.Apache.REEF.Evaluator.Tests
{
    public sealed class TaskRuntimeTests
    {
        [Fact]
        public void TestTaskRuntimeFields()
        {
            var contextId = Guid.NewGuid().ToString();
            var taskId = Guid.NewGuid().ToString();
            var injector = GetInjector(contextId, taskId);
            var taskRuntime = injector.GetInstance<TaskRuntime>();
            Assert.Equal(taskRuntime.TaskId, taskId);
            Assert.Equal(taskRuntime.ContextId, contextId);
        }

        [Fact]
        public void TestTaskRuntimeInitialization()
        {
            var injector = GetInjector();
            var taskRuntime = injector.GetInstance<TaskRuntime>();
            Assert.Equal(taskRuntime.GetTaskState(), TaskState.Init);
            Assert.False(taskRuntime.HasEnded());
        }

        [Fact]
        public void TestTaskRuntimeRunsAndDisposesTask()
        {
            var injector = GetInjector();
            var taskRuntime = injector.GetInstance<TaskRuntime>();
            taskRuntime.RunTask();
            var task = injector.GetInstance<TestTask>();
            task.FinishCountdownEvent.Wait();
            task.DisposeCountdownEvent.Wait();
            Assert.Equal(taskRuntime.GetTaskState(), TaskState.Done);
            Assert.True(taskRuntime.HasEnded());
        }

        [Fact]
        public void TestTaskRuntimeFailure()
        {
            var injector = GetInjector(typeof(ExceptionAction));
            var taskRuntime = injector.GetInstance<TaskRuntime>();
            taskRuntime.RunTask();
            var task = injector.GetInstance<TestTask>();
            task.DisposeCountdownEvent.Wait();
            Assert.Equal(taskRuntime.GetTaskState(), TaskState.Failed);
            Assert.True(taskRuntime.HasEnded());
        }

        [Fact]
        public void TestTaskLifeCycle()
        {
            var contextId = Guid.NewGuid().ToString();
            var taskId = Guid.NewGuid().ToString();

            var injector = GetInjector(typeof(CountDownAction), contextId, taskId);
            var taskRuntime = injector.GetInstance<TaskRuntime>();

            var statusProto = taskRuntime.GetStatusProto();
            Assert.Equal(statusProto.task_id, taskId);
            Assert.Equal(statusProto.context_id, contextId);
            Assert.Equal(statusProto.state, State.INIT);
            Assert.Equal(taskRuntime.GetTaskState(), TaskState.Init);

            taskRuntime.RunTask();
            Assert.Equal(taskRuntime.GetStatusProto().state, State.RUNNING);
            Assert.Equal(taskRuntime.GetTaskState(), TaskState.Running);

            injector.GetInstance<CountDownAction>().CountdownEvent.Signal();

            var task = injector.GetInstance<TestTask>();
            task.FinishCountdownEvent.Wait();
            task.DisposeCountdownEvent.Wait();

            Assert.Equal(taskRuntime.GetStatusProto().state, State.DONE);
            Assert.Equal(taskRuntime.GetTaskState(), TaskState.Done);
        }

        private static IInjector GetInjector(string contextId = "contextId", string taskId = "taskId")
        {
            return GetInjector(typeof(DefaultAction), contextId, taskId);
        }

        private static IInjector GetInjector(Type actionType, string contextId = "contextId", string taskId = "taskId")
        {
            var confBuilder = TangFactory.GetTang().NewConfigurationBuilder();
            var heartbeatManager = Substitute.For<IHeartBeatManager>();

            var evaluatorConfig = confBuilder
                .BindNamedParameter(typeof(ContextConfigurationOptions.ContextIdentifier), contextId)
                .BindNamedParameter(typeof(TaskConfigurationOptions.Identifier), taskId)
                .BindImplementation(typeof(ITask), typeof(TestTask))
                .BindImplementation(typeof(IAction), actionType)
                .Build();

            var injector = TangFactory.GetTang().NewInjector(evaluatorConfig);
            injector.BindVolatileInstance(GenericType<IHeartBeatManager>.Class, heartbeatManager);

            return injector;
        }

        private sealed class TestTask : ITask
        {
            private readonly IAction _action;

            [Inject]
            private TestTask(IAction action)
            {
                FinishCountdownEvent = new CountdownEvent(1);
                DisposeCountdownEvent = new CountdownEvent(1);
                _action = action;
            }

            public CountdownEvent FinishCountdownEvent { get; private set; }

            public CountdownEvent DisposeCountdownEvent { get; private set; }

            public void Dispose()
            {
                DisposeCountdownEvent.Signal();
            }

            public byte[] Call(byte[] memento)
            {
                try
                {
                    _action.Value();
                }
                finally
                {
                    FinishCountdownEvent.Signal();
                }

                return null;
            }
        }

        private interface IAction
        {
            Action Value { get; } 
        }

        private sealed class DefaultAction : IAction
        {
            [Inject]
            private DefaultAction()
            {
            }

            public Action Value
            {
                get
                {
                    // NOOP
                    return () => { };
                }
            }
        }

        private sealed class ExceptionAction : IAction
        {
            [Inject]
            private ExceptionAction()
            {
            }

            public Action Value 
            { 
                get
                {
                    return () =>
                    {
                        throw new Exception();
                    };
                } 
            }
        }

        private sealed class CountDownAction : IAction
        {
            [Inject]
            private CountDownAction()
            {
                CountdownEvent = new CountdownEvent(1);
            }

            public Action Value
            {
                get
                {
                    return () =>
                    {
                        CountdownEvent.Wait();
                    };
                }
            }

            public CountdownEvent CountdownEvent { get; private set; }
        }
    }
}
