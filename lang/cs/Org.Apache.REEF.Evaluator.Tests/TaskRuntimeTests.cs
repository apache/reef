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
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using NSubstitute;
using Org.Apache.REEF.Common.Context;
using Org.Apache.REEF.Common.Protobuf.ReefProtocol;
using Org.Apache.REEF.Common.Runtime.Evaluator;
using Org.Apache.REEF.Common.Runtime.Evaluator.Task;
using Org.Apache.REEF.Common.Tasks;
using Org.Apache.REEF.Common.Tasks.Events;
using Org.Apache.REEF.Driver.Task;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Tang.Util;
using Org.Apache.REEF.Utilities;
using Xunit;

namespace Org.Apache.REEF.Evaluator.Tests
{
    /// <summary>
    /// Tests for TaskRuntime and Task events.
    /// </summary>
    public sealed class TaskRuntimeTests
    {
        /// <summary>
        /// Tests that Task ID and Context ID are properly passed to TaskRuntime.
        /// </summary>
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

        /// <summary>
        /// Tests that TaskRuntime has proper state at initialization.
        /// </summary>
        [Fact]
        public void TestTaskRuntimeInitialization()
        {
            var injector = GetInjector();
            var taskRuntime = injector.GetInstance<TaskRuntime>();
            Assert.Equal(taskRuntime.GetTaskState(), TaskState.Init);
            Assert.False(taskRuntime.HasEnded());
        }

        /// <summary>
        /// Tests a simple Task on TaskRuntime and tests that the Task is
        /// properly disposed.
        /// </summary>
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

        /// <summary>
        /// Tests the correctness of TaskRuntime state on Task failure.
        /// </summary>
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

        /// <summary>
        /// Tests the correctness of TaskRuntime state throughout the lifecycle
        /// of a Task. Also tests that the Task runs properly.
        /// </summary>
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

            var taskInterface = injector.GetInstance<ITask>();
            Assert.True(taskInterface is TestTask);

            var task = taskInterface as TestTask;
            if (task == null)
            {
                throw new Exception("Task is expected to be an instance of TestTask.");
            }

            task.FinishCountdownEvent.Wait();
            task.DisposeCountdownEvent.Wait();

            Assert.Equal(taskRuntime.GetStatusProto().state, State.DONE);
            Assert.Equal(taskRuntime.GetTaskState(), TaskState.Done);
        }

        /// <summary>
        /// Tests whether task start and stop handlers are properly instantiated and invoked
        /// in the happy path.
        /// </summary>
        [Fact]
        public void TestTaskEvents()
        {
            var contextId = Guid.NewGuid().ToString();
            var taskId = Guid.NewGuid().ToString();

            var injector = GetInjector(typeof(CountDownAction), contextId, taskId);
            var taskRuntime = injector.GetInstance<TaskRuntime>();
            var startHandlers = injector.GetNamedInstance<TaskConfigurationOptions.StartHandlers, ISet<IObserver<ITaskStart>>>();

            Assert.Equal(startHandlers.Count, 1);

            var testTaskEventStartHandler = startHandlers.Single() as TestTaskEventHandler;
            Assert.NotNull(testTaskEventStartHandler);
            if (testTaskEventStartHandler == null)
            {
                throw new Exception("Event handler is not expected to be null.");
            }

            taskRuntime.RunTask();

            Assert.True(testTaskEventStartHandler.StartInvoked.IsPresent());
            Assert.Equal(testTaskEventStartHandler.StartInvoked.Value, taskId);
            Assert.False(testTaskEventStartHandler.StopInvoked.IsPresent());

            var countDownAction = injector.GetInstance<CountDownAction>();
            countDownAction.CountdownEvent.Signal();

            var task = injector.GetInstance<TestTask>();
            task.FinishCountdownEvent.Wait();
            task.DisposeCountdownEvent.Wait();

            var stopHandlers = injector.GetNamedInstance<TaskConfigurationOptions.StopHandlers, ISet<IObserver<ITaskStop>>>();

            Assert.Equal(stopHandlers.Count, 1);

            var testTaskEventStopHandler = stopHandlers.Single() as TestTaskEventHandler;
            Assert.NotNull(testTaskEventStopHandler);
            if (testTaskEventStopHandler == null)
            {
                throw new Exception("Event handler is not expected to be null.");
            }

            Assert.True(ReferenceEquals(testTaskEventStartHandler, testTaskEventStopHandler));
            Assert.True(testTaskEventStopHandler.StopInvoked.IsPresent());
            Assert.Equal(testTaskEventStopHandler.StopInvoked.Value, taskId);
        }

        /// <summary>
        /// Tests whether task start and stop handlers are properly instantiated and invoked
        /// on the failure of a task.
        /// </summary>
        [Fact]
        public void TestTaskEventsOnFailure()
        {
            var contextId = Guid.NewGuid().ToString();
            var taskId = Guid.NewGuid().ToString();

            var injector = GetInjector(typeof(ExceptionAction), contextId, taskId);
            var taskRuntime = injector.GetInstance<TaskRuntime>();

            taskRuntime.RunTask();

            var task = injector.GetInstance<TestTask>();
            task.FinishCountdownEvent.Wait();
            task.DisposeCountdownEvent.Wait();

            var stopHandlers = injector.GetNamedInstance<TaskConfigurationOptions.StopHandlers, ISet<IObserver<ITaskStop>>>();
            var testTaskEventStopHandler = stopHandlers.Single() as TestTaskEventHandler;
            Assert.NotNull(testTaskEventStopHandler);
            if (testTaskEventStopHandler == null)
            {
                throw new Exception("Event handler is not expected to be null.");
            }

            Assert.True(testTaskEventStopHandler.StopInvoked.IsPresent());
            Assert.Equal(testTaskEventStopHandler.StopInvoked.Value, taskId);
        }

        /// <summary>
        /// Tests that suspend ends the task and invokes the right handler.
        /// </summary>
        [Fact]
        public void TestSuspendTask()
        {
            var contextId = Guid.NewGuid().ToString();
            var taskId = Guid.NewGuid().ToString();

            var injector = GetInjector(typeof(CountDownAction), contextId, taskId);
            var taskRuntime = injector.GetInstance<TaskRuntime>();
            taskRuntime.RunTask();

            var taskInterface = injector.GetInstance<ITask>();
            Assert.True(taskInterface is TestTask);

            var task = taskInterface as TestTask;
            if (task == null)
            {
                throw new Exception("Task is expected to be an instance of TestTask.");
            }

            taskRuntime.Suspend(null);

            task.FinishCountdownEvent.Wait();
            task.DisposeCountdownEvent.Wait();

            Assert.True(task.SuspendInvoked);
        }

        /// <summary>
        /// Tests that suspend is not invoked after task is done.
        /// </summary>
        [Fact]
        public void TestSuspendTaskAfterDoneIsNotSuspended()
        {
            var contextId = Guid.NewGuid().ToString();
            var taskId = Guid.NewGuid().ToString();

            var injector = GetInjector(contextId, taskId);
            var taskRuntime = injector.GetInstance<TaskRuntime>();
            taskRuntime.RunTask();

            var taskInterface = injector.GetInstance<ITask>();
            Assert.True(taskInterface is TestTask);

            var task = taskInterface as TestTask;
            if (task == null)
            {
                throw new Exception("Task is expected to be an instance of TestTask.");
            }

            task.FinishCountdownEvent.Wait();
            task.DisposeCountdownEvent.Wait();

            var stopHandlers = injector.GetNamedInstance<TaskConfigurationOptions.StopHandlers, ISet<IObserver<ITaskStop>>>();

            var testTaskEventStopHandler = stopHandlers.Single() as TestTaskEventHandler;
            if (testTaskEventStopHandler == null)
            {
                throw new Exception("Event handler is not expected to be null.");
            }

            Assert.Equal(testTaskEventStopHandler.StopInvoked.Value, taskId);

            taskRuntime.Suspend(null);
            Assert.False(task.SuspendInvoked);
        }

        /// <summary>
        /// Tests that suspend is not invoked after task is done.
        /// </summary>
        [Fact]
        public void TestSuspendTaskAfterFailureIsNotSuspended()
        {
            var contextId = Guid.NewGuid().ToString();
            var taskId = Guid.NewGuid().ToString();

            var injector = GetInjector(typeof(ExceptionAction), contextId, taskId);
            var taskRuntime = injector.GetInstance<TaskRuntime>();
            taskRuntime.RunTask();

            var task = injector.GetInstance<TestTask>();

            task.DisposeCountdownEvent.Wait();

            var stopHandlers = injector.GetNamedInstance<TaskConfigurationOptions.StopHandlers, ISet<IObserver<ITaskStop>>>();

            var testTaskEventStopHandler = stopHandlers.Single() as TestTaskEventHandler;
            if (testTaskEventStopHandler == null)
            {
                throw new Exception("Event handler is not expected to be null.");
            }

            Assert.True(testTaskEventStopHandler.StopInvoked.IsPresent());
            Assert.Equal(taskRuntime.GetTaskState(), TaskState.Failed);

            taskRuntime.Suspend(null);
            Assert.False(task.SuspendInvoked);
        }

        private static IInjector GetInjector(string contextId = "contextId", string taskId = "taskId")
        {
            return GetInjector(typeof(DefaultAction), contextId, taskId);
        }

        private static IInjector GetInjector(Type actionType, string contextId = "contextId", string taskId = "taskId")
        {
            var confBuilder = TangFactory.GetTang().NewConfigurationBuilder();
            var heartbeatManager = Substitute.For<IHeartBeatManager>();

            var contextConfig = ContextConfiguration.ConfigurationModule
                .Set(ContextConfiguration.Identifier, contextId)
                .Build();

            var taskConfig = TaskConfiguration.ConfigurationModule
                .Set(TaskConfiguration.Identifier, taskId)
                .Set(TaskConfiguration.OnTaskStart, GenericType<TestTaskEventHandler>.Class)
                .Set(TaskConfiguration.OnTaskStop, GenericType<TestTaskEventHandler>.Class)
                .Set(TaskConfiguration.Task, GenericType<TestTask>.Class)
                .Set(TaskConfiguration.OnSuspend, GenericType<TestTask>.Class)
                .Build();
            
            var actionConfig = confBuilder
                .BindImplementation(typeof(IAction), actionType)
                .Build();

            var injector = TangFactory.GetTang().NewInjector(contextConfig, taskConfig, actionConfig);
            injector.BindVolatileInstance(GenericType<IHeartBeatManager>.Class, heartbeatManager);

            return injector;
        }

        private sealed class TestTaskEventHandler : IObserver<ITaskStart>, IObserver<ITaskStop>
        {
            [Inject]
            private TestTaskEventHandler()
            {
                StartInvoked = Optional<string>.Empty();
                StopInvoked = Optional<string>.Empty();
            }

            public Optional<string> StartInvoked { get; private set; }

            public Optional<string> StopInvoked { get; private set; }

            public void OnNext(ITaskStart value)
            {
                StartInvoked = Optional<string>.Of(value.Id);
            }

            public void OnNext(ITaskStop value)
            {
                StopInvoked = Optional<string>.Of(value.Id);
            }

            public void OnError(Exception error)
            {
                throw new NotImplementedException();
            }

            public void OnCompleted()
            {
                throw new NotImplementedException();
            }
        }

        private sealed class TestTask : ITask, IObserver<ISuspendEvent>
        {
            private readonly IAction _action;

            [Inject]
            private TestTask(IAction action)
            {
                FinishCountdownEvent = new CountdownEvent(1);
                DisposeCountdownEvent = new CountdownEvent(1);
                _action = action;
            }

            public bool SuspendInvoked { get; private set; }

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

            public void OnNext(ISuspendEvent value)
            {
                _action.OnSuspend();
                SuspendInvoked = true;
            }

            public void OnError(Exception error)
            {
                throw new NotImplementedException();
            }

            public void OnCompleted()
            {
                throw new NotImplementedException();
            }
        }

        private interface IAction
        {
            Action Value { get; }

            void OnSuspend();
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

            public void OnSuspend()
            {
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

            public void OnSuspend()
            {
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

            public void OnSuspend()
            {
                CountdownEvent.Signal();
            }

            public CountdownEvent CountdownEvent { get; private set; }
        }
    }
}
