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
using System.Threading.Tasks;
using NSubstitute;
using Org.Apache.REEF.Common.Context;
using Org.Apache.REEF.Common.Events;
using Org.Apache.REEF.Common.Runtime.Evaluator;
using Org.Apache.REEF.Common.Runtime.Evaluator.Context;
using Org.Apache.REEF.Common.Runtime.Evaluator.Utils;
using Org.Apache.REEF.Common.Services;
using Org.Apache.REEF.Common.Tasks;
using Org.Apache.REEF.Common.Tasks.Events;
using Org.Apache.REEF.Evaluator.Tests.TestUtils;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Formats;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Tang.Util;
using Org.Apache.REEF.Utilities;
using Xunit;

namespace Org.Apache.REEF.Evaluator.Tests
{
    public sealed class EvaluatorServiceTests
    {
        [Fact]
        [Trait("Priority", "0")]
        [Trait("Category", "Unit")]
        public void TestServiceInstantiatedAndDisposed()
        {
            var serviceConfiguration = ServiceConfiguration.ConfigurationModule
                .Set(ServiceConfiguration.Services, GenericType<TestService>.Class)
                .Build();

            var serviceInjector = TangFactory.GetTang().NewInjector(serviceConfiguration);
            var contextConfig = GetContextConfiguration();

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
        public void TestServiceContextEventHandlersTriggered()
        {
            var launcher = GetRootContextLauncher(
                GetContextConfiguration(), GetServiceConfiguration(), Optional<IConfiguration>.Empty());

            IInjector serviceInjector = null;
            IInjector contextInjector = null;

            using (var rootContext = launcher.GetRootContext())
            {
                serviceInjector = rootContext.ServiceInjector;
                contextInjector = rootContext.ContextInjector;
                Assert.NotNull(serviceInjector);
                Assert.NotNull(contextInjector);
            }

            var serviceContextStartHandlers =
                serviceInjector.GetNamedInstance<ContextConfigurationOptions.StartHandlers, ISet<IObserver<IContextStart>>>();

            var contextContextStartHandlers = 
                contextInjector.GetNamedInstance<ContextConfigurationOptions.StartHandlers, ISet<IObserver<IContextStart>>>();

            Assert.Equal(1, serviceContextStartHandlers.Count);
            Assert.Equal(2, contextContextStartHandlers.Count);

            var serviceContextStartHandler = serviceContextStartHandlers.First() as TestServiceEventHandlers;

            Assert.True(contextContextStartHandlers.Contains(serviceContextStartHandler));

            var serviceContextStopHandlers =
                serviceInjector.GetNamedInstance<ContextConfigurationOptions.StopHandlers, ISet<IObserver<IContextStop>>>();

            var contextContextStopHandlers = 
                contextInjector.GetNamedInstance<ContextConfigurationOptions.StopHandlers, ISet<IObserver<IContextStop>>>();

            Assert.Equal(1, serviceContextStopHandlers.Count);
            Assert.Equal(2, contextContextStopHandlers.Count);

            var serviceContextStopHandler = serviceContextStopHandlers.First() as TestServiceEventHandlers;

            Assert.True(contextContextStopHandlers.Contains(serviceContextStopHandler));

            foreach (var contextStartHandler in contextContextStartHandlers.Select(h => h as ITestContextEventHandler))
            {
                Assert.NotNull(contextStartHandler);
                Assert.Equal(1, contextStartHandler.ContextStartInvoked);
                Assert.Equal(1, contextStartHandler.ContextStopInvoked);
            }

            foreach (var contextStopHandler in contextContextStopHandlers.Select(h => h as ITestContextEventHandler))
            {
                Assert.NotNull(contextStopHandler);
                Assert.Equal(1, contextStopHandler.ContextStartInvoked);
                Assert.Equal(1, contextStopHandler.ContextStopInvoked);
            }
        }

        [Fact]
        [Trait("Priority", "0")]
        [Trait("Category", "Unit")]
        public void TestServiceContextEventHandlersTriggeredSuccessiveContexts()
        {
            var launcher = GetRootContextLauncher(
                GetContextConfiguration(), GetServiceConfiguration(), Optional<IConfiguration>.Empty());

            IInjector serviceInjector = null;
            IInjector firstContextInjector = null;
            IInjector secondContextInjector = null;

            using (var rootContext = launcher.GetRootContext())
            {
                serviceInjector = rootContext.ServiceInjector;
                firstContextInjector = rootContext.ContextInjector;
                using (var childContext = rootContext.SpawnChildContext(GetContextConfiguration()))
                {
                    secondContextInjector = childContext.ContextInjector;
                }

                Assert.NotNull(serviceInjector);
                Assert.NotNull(firstContextInjector);
                Assert.NotNull(secondContextInjector);
            }

            var serviceContextStartHandlers =
                serviceInjector.GetNamedInstance<ContextConfigurationOptions.StartHandlers, ISet<IObserver<IContextStart>>>();

            var firstContextContextStartHandlers =
                firstContextInjector.GetNamedInstance<ContextConfigurationOptions.StartHandlers, ISet<IObserver<IContextStart>>>();

            var secondContextContextStartHandlers =
                secondContextInjector.GetNamedInstance<ContextConfigurationOptions.StartHandlers, ISet<IObserver<IContextStart>>>();

            Assert.Equal(1, serviceContextStartHandlers.Count);
            Assert.Equal(2, firstContextContextStartHandlers.Count);
            Assert.Equal(2, secondContextContextStartHandlers.Count);

            var intersectSet = new HashSet<IObserver<IContextStart>>(serviceContextStartHandlers);
            intersectSet.IntersectWith(firstContextContextStartHandlers);
            intersectSet.IntersectWith(secondContextContextStartHandlers);

            var unionSet = new HashSet<IObserver<IContextStart>>(serviceContextStartHandlers);
            unionSet.UnionWith(firstContextContextStartHandlers);
            unionSet.UnionWith(secondContextContextStartHandlers);

            Assert.Equal(1, intersectSet.Count);
            Assert.Equal(3, unionSet.Count);

            var serviceContextHandler = serviceContextStartHandlers.Single() as ITestContextEventHandler;
            var unionContextHandlerSet = new HashSet<ITestContextEventHandler>(
                unionSet.Select(h => h as ITestContextEventHandler).Where(h => h != null));

            Assert.Equal(unionSet.Count, unionContextHandlerSet.Count);
            Assert.True(unionContextHandlerSet.Contains(serviceContextHandler));

            foreach (var handler in unionContextHandlerSet.Where(h => h != null))
            {
                if (ReferenceEquals(handler, serviceContextHandler))
                {
                    Assert.Equal(2, handler.ContextStartInvoked);
                    Assert.Equal(2, handler.ContextStopInvoked);
                }
                else
                {
                    Assert.Equal(1, handler.ContextStartInvoked);
                    Assert.Equal(1, handler.ContextStopInvoked);
                }
            }
        }

        [Fact]
        [Trait("Priority", "0")]
        [Trait("Category", "Unit")]
        public async Task TestServiceTaskEventHandlersTriggered()
        {
            await RunTasksAndVerifyEventHandlers(1);
        }

        [Fact]
        [Trait("Priority", "0")]
        [Trait("Category", "Unit")]
        public async Task TestServiceTaskEventHandlersTriggeredSuccessiveTasks()
        {
            await RunTasksAndVerifyEventHandlers(5);
        }

        private static async Task RunTasksAndVerifyEventHandlers(int tasksRun)
        {
            var launcher = GetRootContextLauncher(
               GetContextConfiguration(), GetServiceConfiguration(), Optional<IConfiguration>.Of(GetTaskConfiguration()));

            IInjector serviceInjector = null;

            using (var rootContext = launcher.GetRootContext())
            {
                serviceInjector = rootContext.ServiceInjector;
                for (var i = 0; i < tasksRun; i++)
                {
                    await rootContext.StartTask(launcher.RootTaskConfig.Value);
                }

                Assert.NotNull(serviceInjector);
            }

            var serviceTaskStartHandlers =
                serviceInjector.GetNamedInstance<TaskConfigurationOptions.StartHandlers, ISet<IObserver<ITaskStart>>>();

            Assert.Equal(1, serviceTaskStartHandlers.Count);

            var serviceTaskStartHandler = serviceTaskStartHandlers.First() as TestServiceEventHandlers;

            var serviceTaskStopHandlers =
                serviceInjector.GetNamedInstance<TaskConfigurationOptions.StopHandlers, ISet<IObserver<ITaskStop>>>();

            Assert.Equal(1, serviceTaskStopHandlers.Count);

            var serviceTaskStopHandler = serviceTaskStopHandlers.First() as TestServiceEventHandlers;

            Assert.Equal(serviceTaskStopHandler, serviceTaskStartHandler);

            Assert.NotNull(serviceTaskStartHandler);

            if (serviceTaskStartHandler == null || serviceTaskStopHandler == null)
            {
                // Get rid of warning.
                throw new Exception();
            }

            Assert.Equal(tasksRun, serviceTaskStartHandler.TaskStartInvoked);
            Assert.Equal(tasksRun, serviceTaskStopHandler.TaskStopInvoked);
        }

        private static RootContextLauncher GetRootContextLauncher(
            IConfiguration contextConfig, IConfiguration serviceConfig, Optional<IConfiguration> taskConfig)
        {
            var injector = TangFactory.GetTang().NewInjector();
            var serializer = injector.GetInstance<AvroConfigurationSerializer>();
            var contextConfigStr = serializer.ToString(contextConfig);
            var serviceConfigStr = serializer.ToString(serviceConfig);
            var taskConfigStr = Optional<string>.Empty();
            if (taskConfig.IsPresent())
            {
                taskConfigStr = Optional<string>.Of(serializer.ToString(taskConfig.Value));
            }

            var contextLauncherConfigBuilder = TangFactory.GetTang().NewConfigurationBuilder()
                .BindNamedParameter<RootContextConfiguration, string>(GenericType<RootContextConfiguration>.Class, contextConfigStr)
                .BindNamedParameter<RootServiceConfiguration, string>(GenericType<RootServiceConfiguration>.Class, serviceConfigStr);

            if (taskConfigStr.IsPresent())
            {
                contextLauncherConfigBuilder = contextLauncherConfigBuilder
                    .BindNamedParameter<InitialTaskConfiguration, string>(GenericType<InitialTaskConfiguration>.Class, taskConfigStr.Value);
            }

            injector = injector.ForkInjector(contextLauncherConfigBuilder.Build());
            var heartbeatManager = Substitute.For<IHeartBeatManager>();
            injector.BindVolatileInstance(GenericType<IHeartBeatManager>.Class, heartbeatManager);
            return injector.GetInstance<RootContextLauncher>();
        }

        private static IConfiguration GetTaskConfiguration()
        {
            return TaskConfiguration.ConfigurationModule
                .Set(TaskConfiguration.Identifier, "ID")
                .Set(TaskConfiguration.Task, GenericType<SimpleTestTask>.Class)
                .Set(TaskConfiguration.OnTaskStart, GenericType<TestTaskEventHandlers>.Class)
                .Set(TaskConfiguration.OnTaskStop, GenericType<TestTaskEventHandlers>.Class)
                .Build();
        }
        
        private static IConfiguration GetContextConfiguration()
        {
            return ContextConfiguration.ConfigurationModule
                .Set(ContextConfiguration.Identifier, "ID")
                .Set(ContextConfiguration.OnContextStart, GenericType<TestContextEventHandlers>.Class)
                .Set(ContextConfiguration.OnContextStop, GenericType<TestContextEventHandlers>.Class)
                .Build();
        }

        private static IConfiguration GetServiceConfiguration()
        {
            return ServiceConfiguration.ConfigurationModule
                .Set(ServiceConfiguration.OnContextStarted, GenericType<TestServiceEventHandlers>.Class)
                .Set(ServiceConfiguration.OnContextStop, GenericType<TestServiceEventHandlers>.Class)
                .Set(ServiceConfiguration.OnTaskStarted, GenericType<TestServiceEventHandlers>.Class)
                .Set(ServiceConfiguration.OnTaskStop, GenericType<TestServiceEventHandlers>.Class)
                .Build();
        }

        private interface ITestContextEventHandler : IObserver<IContextStart>, IObserver<IContextStop>
        {
            int ContextStartInvoked { get; }

            int ContextStopInvoked { get; }
        }

        private interface ITestTaskEventHandler : IObserver<ITaskStart>, IObserver<ITaskStop>
        {
            int TaskStartInvoked { get; }

            int TaskStopInvoked { get; }
        }

        private sealed class TestServiceEventHandlers : ITestContextEventHandler, ITestTaskEventHandler
        {
            public int ContextStartInvoked { get; private set; }

            public int ContextStopInvoked { get; private set; }

            public int TaskStartInvoked { get; private set; }

            public int TaskStopInvoked { get; private set; }

            [Inject]
            private TestServiceEventHandlers()
            {
            }

            public void OnNext(IContextStart value)
            {
                ContextStartInvoked++;
            }

            public void OnNext(IContextStop value)
            {
                ContextStopInvoked++;
            }

            public void OnNext(ITaskStart value)
            {
                TaskStartInvoked++;
            }

            public void OnNext(ITaskStop value)
            {
                TaskStopInvoked++;
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

        private class TestContextEventHandlers : ITestContextEventHandler
        {
            public int ContextStartInvoked { get; private set; }

            public int ContextStopInvoked { get; private set; }

            [Inject]
            private TestContextEventHandlers()
            {
            }

            public void OnNext(IContextStart value)
            {
                ContextStartInvoked++;
            }

            public void OnNext(IContextStop value)
            {
                ContextStopInvoked++;
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

        private class TestTaskEventHandlers : ITestTaskEventHandler
        {
            public int TaskStartInvoked { get; private set; }

            public int TaskStopInvoked { get; private set; }

            [Inject]
            private TestTaskEventHandlers()
            {
            }

            public void OnNext(ITaskStart value)
            {
                TaskStartInvoked++;
            }

            public void OnNext(ITaskStop value)
            {
                TaskStopInvoked++;
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
