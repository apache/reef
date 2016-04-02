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
using System.Text;
using System.Threading;
using NSubstitute;
using Org.Apache.REEF.Common.Context;
using Org.Apache.REEF.Common.Events;
using Org.Apache.REEF.Common.Protobuf.ReefProtocol;
using Org.Apache.REEF.Common.Runtime.Evaluator;
using Org.Apache.REEF.Common.Runtime.Evaluator.Context;
using Org.Apache.REEF.Common.Services;
using Org.Apache.REEF.Common.Tasks;
using Org.Apache.REEF.Common.Tasks.Events;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Implementations.Configuration;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Tang.Util;
using Org.Apache.REEF.Utilities;
using Xunit;
using ContextConfiguration = Org.Apache.REEF.Common.Context.ContextConfiguration;

namespace Org.Apache.REEF.Evaluator.Tests
{
    public sealed class ContextRuntimeTests
    {
        [Fact]
        [Trait("Priority", "0")]
        [Trait("Category", "Unit")]
        public void TestContextEvents()
        {
            const string hello = "Hello!";
            var contextConfig = GetContextEventHandlerContextConfiguration();
            var injector = TangFactory.GetTang().NewInjector();

            var handler = new TestContextEventHandler();
            injector.BindVolatileInstance(GenericType<TestContextEventHandler>.Class, handler);

            using (var contextRuntime = new ContextRuntime(injector, contextConfig,
                    Optional<ContextRuntime>.Empty()))
            {
                contextRuntime.HandleContextMessage(Encoding.UTF8.GetBytes(hello));
            }

            Assert.True(handler.Started, "Handler did not receive the start signal.");
            Assert.True(handler.Stopped, "Handler did not receive the stop signal.");
            Assert.Equal(Encoding.UTF8.GetString(handler.MessageReceived), hello);
        }

        [Fact]
        [Trait("Priority", "0")]
        [Trait("Category", "Unit")]
        public void TestContextBindServiceBoundInterfaceThrowsException()
        {
            var boundTestServiceConfig = TangFactory.GetTang()
                .NewConfigurationBuilder()
                .BindImplementation(GenericType<ITestService>.Class, GenericType<TestService>.Class)
                .Build();

            var serviceConfiguration = ServiceConfiguration.ConfigurationModule.Build();
            var serviceInjector = TangFactory.GetTang().NewInjector(Configurations.Merge(serviceConfiguration, boundTestServiceConfig));
            var contextConfig = Configurations.Merge(GetContextEventHandlerContextConfiguration(), boundTestServiceConfig);

            var ex = Record.Exception(() => new ContextRuntime(serviceInjector, contextConfig, Optional<ContextRuntime>.Empty()));

            // This should throw an Exception because we are binding the interface ITestService twice, once in serviceConfiguration
            // and once in contextConfiguration. The Context injector is forked from the ServiceInjector, which already has the 
            // interface bound.
            Assert.True(ex != null);
        }

        [Fact]
        [Trait("Priority", "0")]
        [Trait("Category", "Unit")]
        public void TestServiceInstantiatedAndDisposed()
        {
            var serviceConfiguration = ServiceConfiguration.ConfigurationModule
                .Set(ServiceConfiguration.Services, GenericType<TestService>.Class)
                .Build();

            var serviceInjector = TangFactory.GetTang().NewInjector(serviceConfiguration);
            var contextConfig = GetContextEventHandlerContextConfiguration();

            TestService testService;
            using (var contextRuntime = new ContextRuntime(serviceInjector, contextConfig, Optional<ContextRuntime>.Empty()))
            {
                var servicesFromInjector = serviceInjector.GetNamedInstance<ServicesSet, ISet<object>>();
                testService = servicesFromInjector.Single() as TestService;
                Assert.NotNull(testService);
                if (testService == null)
                {
                    // Not possible
                    return;
                }

                var testServiceFromInjector = serviceInjector.GetInstance<TestService>();
                Assert.True(ReferenceEquals(testService, testServiceFromInjector));

                var contextTestService = contextRuntime.ContextInjector.GetInstance<TestService>();
                Assert.True(ReferenceEquals(contextTestService, testServiceFromInjector));
            }

            Assert.True(testService.Disposed);
        }

        [Fact]
        [Trait("Priority", "0")]
        [Trait("Category", "Unit")]
        public void TestBaseServiceWithContextStacking()
        {
            var serviceConfiguration = ServiceConfiguration.ConfigurationModule
                .Set(ServiceConfiguration.Services, GenericType<TestService>.Class)
                .Build();

            var serviceInjector = TangFactory.GetTang().NewInjector(serviceConfiguration);
            var contextConfig = GetContextEventHandlerContextConfiguration();

            using (var contextRuntime = new ContextRuntime(serviceInjector, contextConfig, Optional<ContextRuntime>.Empty()))
            {
                var childContextConfiguration = ContextConfiguration.ConfigurationModule
                    .Set(ContextConfiguration.Identifier, "Context2")
                    .Build();

                using (var childContextRuntime = contextRuntime.SpawnChildContext(childContextConfiguration))
                {
                    var servicesFromInjector = serviceInjector.GetNamedInstance<ServicesSet, ISet<object>>();

                    // Check that parent service injector does not contain instances of SecondTestService
                    Assert.False(servicesFromInjector.OfType<SecondTestService>().Any());

                    var contextTestService = childContextRuntime.ContextInjector.GetInstance<TestService>();
                    Assert.True(ReferenceEquals(contextTestService, serviceInjector.GetInstance<TestService>()));
                }
            }
        }

        [Fact]
        [Trait("Priority", "0")]
        [Trait("Category", "Unit")]
        public void TestContextStackingDoesNotGetSameInstance()
        {
            var serviceInjector = TangFactory.GetTang().NewInjector();
            var contextConfig = GetContextEventHandlerContextConfiguration();

            using (var contextRuntime = new ContextRuntime(serviceInjector, contextConfig, Optional<ContextRuntime>.Empty()))
            {
                var childContextConfiguration = GetContextEventHandlerContextConfiguration();
                using (var childContextRuntime = contextRuntime.SpawnChildContext(childContextConfiguration))
                {
                    Assert.False(ReferenceEquals(
                        contextRuntime.ContextInjector.GetInstance<TestContextEventHandler>(),
                        childContextRuntime.ContextInjector.GetInstance<TestContextEventHandler>()));
                }
            }
        }

        [Fact]
        [Trait("Priority", "0")]
        [Trait("Category", "Unit")]
        public void TestContextStackingParentContext()
        {
            var serviceInjector = TangFactory.GetTang().NewInjector();
            var contextConfig = GetSimpleContextConfiguration();

            using (var contextRuntime = new ContextRuntime(serviceInjector, contextConfig, Optional<ContextRuntime>.Empty()))
            {
                var childContextConfiguration = GetSimpleContextConfiguration();
                using (var childContextRuntime = contextRuntime.SpawnChildContext(childContextConfiguration))
                {
                    Assert.False(contextRuntime.ParentContext.IsPresent());
                    Assert.True(childContextRuntime.ParentContext.IsPresent());
                    Assert.True(ReferenceEquals(contextRuntime, childContextRuntime.ParentContext.Value));
                }
            }
        }

        [Fact]
        [Trait("Priority", "0")]
        [Trait("Category", "Unit")]
        public void TestUnableToSpawnChildWhileTaskIsRunning()
        {
            var serviceInjector = TangFactory.GetTang().NewInjector();
            var contextConfig = GetSimpleContextConfiguration();

            using (var contextRuntime = new ContextRuntime(serviceInjector, contextConfig, Optional<ContextRuntime>.Empty()))
            {
                var taskConfig = TaskConfiguration.ConfigurationModule
                    .Set(TaskConfiguration.Task, GenericType<TestTask>.Class)
                    .Set(TaskConfiguration.Identifier, "ID")
                    .Build();

                try
                {
                    var hbMgr = Substitute.For<IHeartBeatManager>();
                    contextRuntime.ContextInjector.BindVolatileInstance(GenericType<IHeartBeatManager>.Class, hbMgr);
                    contextRuntime.StartTask(taskConfig);

                    Assert.True(contextRuntime.TaskRuntime.IsPresent());
                    Assert.True(contextRuntime.GetTaskStatus().IsPresent());
                    Assert.Equal(contextRuntime.GetTaskStatus().Value.state, State.RUNNING);

                    var childContextConfiguration = GetSimpleContextConfiguration();

                    Assert.Throws<InvalidOperationException>(
                        () => contextRuntime.SpawnChildContext(childContextConfiguration));
                }
                finally
                {
                    var testTask = contextRuntime.TaskRuntime.Value.Task as TestTask;
                    if (testTask == null)
                    {
                        throw new Exception();
                    }

                    testTask.CountDownEvent.Signal();
                    testTask.DisposedEvent.Wait();
                }
            }
        }

        [Fact]
        [Trait("Priority", "0")]
        [Trait("Category", "Unit")]
        public void TestUnableToRunMultipleTasksAtTheSameTime()
        {
            var serviceInjector = TangFactory.GetTang().NewInjector();
            var contextConfig = GetSimpleContextConfiguration();

            using (var contextRuntime = new ContextRuntime(serviceInjector, contextConfig, Optional<ContextRuntime>.Empty()))
            {
                var taskConfig = TaskConfiguration.ConfigurationModule
                    .Set(TaskConfiguration.Task, GenericType<TestTask>.Class)
                    .Set(TaskConfiguration.Identifier, "ID")
                    .Build();

                try
                {
                    var hbMgr = Substitute.For<IHeartBeatManager>();
                    contextRuntime.ContextInjector.BindVolatileInstance(GenericType<IHeartBeatManager>.Class, hbMgr);
                    contextRuntime.StartTask(taskConfig);

                    Assert.True(contextRuntime.TaskRuntime.IsPresent());
                    Assert.True(contextRuntime.GetTaskStatus().IsPresent());
                    Assert.Equal(contextRuntime.GetTaskStatus().Value.state, State.RUNNING);

                    Assert.Throws<InvalidOperationException>(
                        () => contextRuntime.StartTask(taskConfig));
                }
                finally
                {
                    var testTask = contextRuntime.TaskRuntime.Value.Task as TestTask;
                    if (testTask == null)
                    {
                        throw new Exception();
                    }

                    testTask.CountDownEvent.Signal();
                    testTask.DisposedEvent.Wait();
                }
            }
        }

        [Fact]
        [Trait("Priority", "0")]
        [Trait("Category", "Unit")]
        public void TestTwoSuccessiveTasksOnSameContext()
        {
            var serviceInjector = TangFactory.GetTang().NewInjector();
            var contextConfig = GetSimpleContextConfiguration();

            using (var contextRuntime = new ContextRuntime(serviceInjector, contextConfig, Optional<ContextRuntime>.Empty()))
            {
                var taskConfig = TaskConfiguration.ConfigurationModule
                    .Set(TaskConfiguration.Task, GenericType<TestTask>.Class)
                    .Set(TaskConfiguration.OnTaskStop, GenericType<TestTask>.Class)
                    .Set(TaskConfiguration.Identifier, "ID")
                    .Build();

                var hbMgr = Substitute.For<IHeartBeatManager>();
                contextRuntime.ContextInjector.BindVolatileInstance(GenericType<IHeartBeatManager>.Class, hbMgr);
                contextRuntime.StartTask(taskConfig);
                var testTask = contextRuntime.TaskRuntime.Value.Task as TestTask;
                if (testTask == null)
                {
                    throw new Exception();
                }

                testTask.CountDownEvent.Signal();
                testTask.StopEvent.Wait();
                Assert.False(contextRuntime.GetTaskStatus().IsPresent());
                testTask.DisposedEvent.Wait();

                contextRuntime.StartTask(taskConfig);
                Assert.Equal(contextRuntime.GetTaskStatus().Value.state, State.RUNNING);

                var secondTestTask = contextRuntime.TaskRuntime.Value.Task as TestTask;
                if (secondTestTask == null)
                {
                    throw new Exception();
                }

                Assert.False(ReferenceEquals(testTask, secondTestTask));

                secondTestTask.CountDownEvent.Signal();
                secondTestTask.StopEvent.Wait();
                Assert.False(contextRuntime.GetTaskStatus().IsPresent());
                secondTestTask.DisposedEvent.Wait();
            }
        }

        [Fact]
        [Trait("Priority", "0")]
        [Trait("Category", "Unit")]
        public void TestServiceStacking()
        {
            var serviceConfiguration = ServiceConfiguration.ConfigurationModule
                .Set(ServiceConfiguration.Services, GenericType<TestService>.Class)
                .Build();

            var serviceInjector = TangFactory.GetTang().NewInjector(serviceConfiguration);
            var contextConfig = GetContextEventHandlerContextConfiguration();

            using (var contextRuntime = new ContextRuntime(serviceInjector, contextConfig, Optional<ContextRuntime>.Empty()))
            {
                var childContextConfiguration = ContextConfiguration.ConfigurationModule
                    .Set(ContextConfiguration.Identifier, "Context2")
                    .Build();

                var childServiceConfiguration = ServiceConfiguration.ConfigurationModule
                    .Set(ServiceConfiguration.Services, GenericType<SecondTestService>.Class)
                    .Build();

                using (var childContextRuntime = contextRuntime.SpawnChildContext(childContextConfiguration, childServiceConfiguration))
                {
                    // Check that parent service injector does not contain instances of SecondTestService
                    Assert.False(contextRuntime.Services.Value.OfType<SecondTestService>().Any());
                    Assert.True(childContextRuntime.Services.Value.OfType<TestService>().Count() == 1);
                    Assert.True(childContextRuntime.Services.Value.OfType<SecondTestService>().Count() == 1);
                    Assert.True(childContextRuntime.Services.Value.Count() == 2);
                    Assert.True(ReferenceEquals(
                        childContextRuntime.ContextInjector.GetInstance<TestService>(),
                        contextRuntime.Services.Value.OfType<TestService>().Single()));
                }
            }
        }

        private static IConfiguration GetSimpleContextConfiguration()
        {
            return ContextConfiguration.ConfigurationModule.Set(ContextConfiguration.Identifier, "ID").Build();
        }

        private static IConfiguration GetContextEventHandlerContextConfiguration()
        {
            return ContextConfiguration.ConfigurationModule
                .Set(ContextConfiguration.Identifier, "ID")
                .Set(ContextConfiguration.OnContextStart, GenericType<TestContextEventHandler>.Class)
                .Set(ContextConfiguration.OnContextStop, GenericType<TestContextEventHandler>.Class)
                .Set(ContextConfiguration.OnMessage, GenericType<TestContextEventHandler>.Class)
                .Build();
        }

        private interface ITestService
        {
            // empty
        }

        internal sealed class TestService : ITestService, IDisposable
        {
            [Inject]
            private TestService()
            {
                Disposed = false;
            }

            public bool Disposed { get; private set; }

            public void Dispose()
            {
                Disposed = true;
            }
        }

        private sealed class SecondTestService
        {
            [Inject]
            private SecondTestService()
            {
            }
        }

        private sealed class TestContextEventHandler 
            : IObserver<IContextStart>, IObserver<IContextStop>, IContextMessageHandler
        {
            [Inject]
            public TestContextEventHandler()
            {
            }

            public bool Started { get; private set; }

            public bool Stopped { get; private set; }

            public byte[] MessageReceived { get; private set; }

            public void OnNext(IContextStart value)
            {
                Started = true;
            }

            public void OnNext(IContextStop value)
            {
                Stopped = true;
            }

            public void OnNext(byte[] value)
            {
                MessageReceived = value;
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

        private sealed class TestTask : ITask, IObserver<ITaskStop>
        {
            [Inject]
            private TestTask()
            {
                CountDownEvent = new CountdownEvent(1);
                StopEvent = new CountdownEvent(1);
                DisposedEvent = new CountdownEvent(1);
            }

            public CountdownEvent CountDownEvent { get; private set; }

            public CountdownEvent StopEvent { get; private set; }

            public CountdownEvent DisposedEvent { get; private set; }

            public void Dispose()
            {
                DisposedEvent.Signal();
            }

            public byte[] Call(byte[] memento)
            {
                CountDownEvent.Wait();
                return null;
            }

            public void OnNext(ITaskStop value)
            {
                StopEvent.Signal();
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
    }
}
