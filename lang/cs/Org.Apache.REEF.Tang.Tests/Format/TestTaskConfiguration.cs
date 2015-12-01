/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

using System;
using System.Collections.Generic;
using Org.Apache.REEF.Common.Tasks;
using Org.Apache.REEF.Examples.Tasks.HelloTask;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Formats;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Tang.Util;
using Xunit;

namespace Org.Apache.REEF.Tang.Tests.Format
{
    public interface ISuspendEvent
    {
        string GetId();
    }

    public interface IDriverMessage
    {
        string GetId();
    }

    public interface ITaskMessageSource
    {
        string GetId();
    }

    public interface ITaskStop
    {
        string GetId();
    }

    public interface IEventHandler<T>
    {
        void OnNext(T t);
    }

    public interface ITaskStart
    {
        string GetId();
    }

    public interface ICloseEvent
    {
        string GetId();
    }

    public class TestTaskConfiguration
    {
        [Fact]
        public void TaskConfigurationTestWith3Parameters()
        {
            TaskConfigurationWith3Parameters.Conf
                .Set(TaskConfigurationWith3Parameters.ONCLOSE, GenericType<TaskCloseHandler>.Class)
                .Build();
        }

        [Fact]
        public void TaskConfigurationWithMyEventHandlerTest()
        {
            TaskConfigurationWithMyEventHandler.Conf
                .Set(TaskConfigurationWithMyEventHandler.ONCLOSE2, GenericType<MyTaskCloseHandler>.Class)
                .Build();
        }

        [Fact]
        public void TaskConfigurationTest()
        {
            IConfiguration conf1 = TaskConfiguration.Conf
                                                    .Set(TaskConfiguration.IDENTIFIER, "sample task")
                                                    .Set(TaskConfiguration.TASK, GenericType<HelloTask>.Class)
                                                    .Set(TaskConfiguration.ONCLOSE, GenericType<TaskCloseHandler>.Class)
                                                    .Set(TaskConfiguration.MEMENTO, "Test")
                                                    .Set(TaskConfiguration.ONSUSPEND, GenericType<SuspendHandler>.Class)
                                                    .Set(TaskConfiguration.ONMESSAGE,
                                                         GenericType<DriverMessageHandler>.Class)
                                                    .Set(TaskConfiguration.ONSENDMESSAGE, GenericType<TaskMsg>.Class)
                                                    .Set(TaskConfiguration.ONTASKSTARTED,
                                                         GenericType<TaskStartHandler>.Class)
                                                    .Set(TaskConfiguration.ONTASKSTOP,
                                                         GenericType<TaskStopHandler>.Class)
                                                    .Build();

            IInjector injector1 = TangFactory.GetTang().NewInjector(conf1);
            var task1 = (HelloTask)injector1.GetInstance(typeof(ITask));
            Assert.NotNull(task1);

            var serializer = new AvroConfigurationSerializer();
            byte[] bytes = serializer.ToByteArray(conf1);
            IConfiguration conf2 = serializer.FromByteArray(bytes);

            IInjector injector2 = TangFactory.GetTang().NewInjector(conf2);
            var task2 = (HelloTask)injector2.GetInstance(typeof(ITask));
            Assert.NotNull(task2);
        }

        [Fact]
        public void TaskConfigurationSerializationTest()
        {
            IConfiguration conf1 = TaskConfiguration.Conf
                .Set(TaskConfiguration.IDENTIFIER, "sample task")
                .Set(TaskConfiguration.TASK, GenericType<HelloTask>.Class)
                .Set(TaskConfiguration.ONCLOSE, GenericType<TaskCloseHandler>.Class)
                .Set(TaskConfiguration.MEMENTO, "Test")
                .Set(TaskConfiguration.ONSUSPEND, GenericType<SuspendHandler>.Class)
                .Set(TaskConfiguration.ONMESSAGE, GenericType<DriverMessageHandler>.Class)
                .Set(TaskConfiguration.ONSENDMESSAGE, GenericType<TaskMsg>.Class)
                .Set(TaskConfiguration.ONTASKSTARTED, GenericType<TaskStartHandler>.Class)
                .Set(TaskConfiguration.ONTASKSTOP, GenericType<TaskStopHandler>.Class)
                .Build();

            IInjector injector1 = TangFactory.GetTang().NewInjector(conf1);
            var task1 = (HelloTask)injector1.GetInstance(typeof(ITask));
            Assert.NotNull(task1);

            var serializer = new AvroConfigurationSerializer();
            byte[] bytes = serializer.ToByteArray(conf1);
            IConfiguration conf2 = serializer.FromByteArray(bytes);

            IInjector injector2 = TangFactory.GetTang().NewInjector(conf2);
            var task2 = (HelloTask)injector2.GetInstance(typeof(ITask));
            Assert.NotNull(task2);

            serializer.ToFileStream(conf1, "TaskConfiguration.bin");
            IConfiguration conf3 = serializer.FromFileStream("TaskConfiguration.bin");

            IInjector injector3 = TangFactory.GetTang().NewInjector(conf3);
            var task3 = (HelloTask)injector3.GetInstance(typeof(ITask));
            Assert.NotNull(task3);
        }
    }

    public class TaskConfigurationWith3Parameters : ConfigurationModuleBuilder
    {
        public static readonly OptionalImpl<IEventHandler<ICloseEvent>> ONCLOSE = new OptionalImpl<IEventHandler<ICloseEvent>>();

        public static ConfigurationModule Conf 
        {
            get
            {
                return new TaskConfigurationWith3Parameters()
                    .BindNamedParameter<TaskConfigurationOptions.CloseHandler, IEventHandler<ICloseEvent>, IEventHandler<ICloseEvent>>(GenericType<TaskConfigurationOptions.CloseHandler>.Class, ONCLOSE)
                    .Build();
            }
        }
    }

    public class TaskConfiguration : ConfigurationModuleBuilder
    {
        public static readonly OptionalImpl<IEventHandler<ICloseEvent>> ONCLOSE = new OptionalImpl<IEventHandler<ICloseEvent>>();
        public static readonly RequiredParameter<string> IDENTIFIER = new RequiredParameter<string>();
        public static readonly RequiredImpl<ITask> TASK = new RequiredImpl<ITask>();
        public static readonly OptionalImpl<IEventHandler<ISuspendEvent>> ONSUSPEND = new OptionalImpl<IEventHandler<ISuspendEvent>>();
        public static readonly OptionalImpl<IEventHandler<IDriverMessage>> ONMESSAGE = new OptionalImpl<IEventHandler<IDriverMessage>>();
        public static readonly OptionalParameter<string> MEMENTO = new OptionalParameter<string>();
        public static readonly OptionalImpl<ITaskMessageSource> ONSENDMESSAGE = new OptionalImpl<ITaskMessageSource>();
        public static readonly OptionalImpl<IEventHandler<ITaskStart>> ONTASKSTARTED = new OptionalImpl<IEventHandler<ITaskStart>>();
        public static readonly OptionalImpl<IEventHandler<ITaskStop>> ONTASKSTOP = new OptionalImpl<IEventHandler<ITaskStop>>();

        public static ConfigurationModule Conf 
        {
            get
            {
                return new TaskConfiguration()
                    .BindNamedParameter(GenericType<TaskConfigurationOptions.Identifier>.Class, IDENTIFIER)
                    .BindImplementation(GenericType<ITask>.Class, TASK)
                    .BindNamedParameter(GenericType<TaskConfigurationOptions.Memento>.Class, MEMENTO)
                    .BindNamedParameter(GenericType<TaskConfigurationOptions.CloseHandler>.Class, ONCLOSE)
                    .BindNamedParameter(GenericType<TaskConfigurationOptions.SuspendHandler>.Class, ONSUSPEND)
                    .BindNamedParameter(GenericType<TaskConfigurationOptions.MessageHandler>.Class, ONMESSAGE)
                    .BindSetEntry(GenericType<TaskConfigurationOptions.TaskMessageSources>.Class, ONSENDMESSAGE)
                    .BindSetEntry(GenericType<TaskConfigurationOptions.StartHandlers>.Class, ONTASKSTARTED)
                    .BindSetEntry(GenericType<TaskConfigurationOptions.StopHandlers>.Class, ONTASKSTOP)
                    .Build();
            }
        }
    }

    public class TaskConfigurationWithMyEventHandler : ConfigurationModuleBuilder
    {
        public static readonly OptionalImpl<MyEventHandler<ICloseEvent>> ONCLOSE2 = new OptionalImpl<MyEventHandler<ICloseEvent>>();

        public static ConfigurationModule Conf 
        {
            get
            {
                return new TaskConfigurationWithMyEventHandler()
                    .BindNamedParameter<MyTaskConfigurationOptions.MyCloseHandler, MyEventHandler<ICloseEvent>, IEventHandler<ICloseEvent>>(GenericType<MyTaskConfigurationOptions.MyCloseHandler>.Class, ONCLOSE2)
                    .Build();
            }
        }
    }

    public class MyEventHandler<T> : IEventHandler<T>
    {
        public void OnNext(T t)
        {
        }
    }
    
    public class TaskConfigurationOptions
    {
        [NamedParameter(DefaultValue = "Unnamed Task", Documentation = "The Identifier of the Task")]
        public class Identifier : Name<string> 
        {
        }

        [NamedParameter(Documentation = "The event handler that receives the close event", DefaultClass = typeof(DefaultCloseHandler))]
        public class CloseHandler : Name<IEventHandler<ICloseEvent>>
        {
        }

        [NamedParameter(Documentation = "The memento to be used for the Task.")]
        public class Memento : Name<string> 
        {
        }

        [NamedParameter(Documentation = "The event handler that receives the suspend event", DefaultClass = typeof(DefaultSuspendHandler))]
        public class SuspendHandler : Name<IEventHandler<ISuspendEvent>> 
        {
        }

        [NamedParameter(Documentation = "The event handler that receives messages from the driver", DefaultClass = typeof(DefaultDriverMessageHandler))]
        public class MessageHandler : Name<IEventHandler<IDriverMessage>> 
        {
        }

        [NamedParameter(Documentation = "TaskMessageSource instances.")]
        public class TaskMessageSources : Name<ISet<ITaskMessageSource>> 
        {
        }

        [NamedParameter(Documentation = "The set of event handlers for the TaskStart event.")]
        public class StartHandlers : Name<ISet<IEventHandler<ITaskStart>>> 
        {
        }

        [NamedParameter(Documentation = "The set of event handlers for the TaskStop event.")]
        public class StopHandlers : Name<ISet<IEventHandler<ITaskStop>>> 
        {
        }
    }

    public class MyTaskConfigurationOptions
    {
        [NamedParameter(Documentation = "The event handler that receives the close event", DefaultClass = typeof(MyDefaultCloseHandler))]
        public class MyCloseHandler : Name<IEventHandler<ICloseEvent>>
        {
        }
    }

    public class DefaultCloseHandler : IEventHandler<ICloseEvent>
    {
        [Inject]
        public DefaultCloseHandler()
        {
        }

        public void OnNext(ICloseEvent closeEvent)
        {
        }
    }

    public class MyDefaultCloseHandler : MyEventHandler<ICloseEvent>
    {
        [Inject]
        public MyDefaultCloseHandler()
        {
        }
    }

    public class TaskCloseHandler : IEventHandler<ICloseEvent>
    {
        [Inject]
        public TaskCloseHandler()
        {
        }

        public void OnNext(ICloseEvent closeEvent)
        {
        }
    }

    public class MyTaskCloseHandler : MyEventHandler<ICloseEvent>
    {
        [Inject]
        public MyTaskCloseHandler()
        {
        }
    }

    public class DefaultSuspendHandler : IEventHandler<ISuspendEvent> 
    {
      [Inject]
      public DefaultSuspendHandler() 
      {
      }

      public void OnNext(ISuspendEvent suspendEvent) 
      {
            throw new Exception("No handler for SuspendEvent registered. event: " + suspendEvent);
      }
    }

    public class SuspendHandler : IEventHandler<ISuspendEvent> 
    {
        public void OnNext(ISuspendEvent suspendEvent) 
        {
        }
    }

    public class DefaultDriverMessageHandler : IEventHandler<IDriverMessage> 
    {
        [Inject]
        public DefaultDriverMessageHandler() 
        {
        }

        public void OnNext(IDriverMessage driverMessage) 
        {
            throw new Exception("No DriverMessage handler bound. Message received:" + driverMessage);
        }
    }

    public class DriverMessageHandler : IEventHandler<IDriverMessage> 
    {
        public void OnNext(IDriverMessage driverMessage) 
        {
        }
    }

    public class TaskStartHandler : IEventHandler<ITaskStart> 
    {
        public void OnNext(ITaskStart t)
        {
            throw new NotImplementedException();
        }
    }

    public class TaskStopHandler : IEventHandler<ITaskStop>
    {
        public void OnNext(ITaskStop t)
        {
            throw new NotImplementedException();
        }
    }
    
    public class TaskMsg : ITask, ITaskMessageSource 
    {
        [Inject]
        public TaskMsg() 
        {
        }

        public byte[] Call(byte[] memento)
        {
            throw new NotImplementedException();
        }

        public string GetId()
        {
            throw new NotImplementedException();
        }

        public void Dispose()
        {
        }
    }
}