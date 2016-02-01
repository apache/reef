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
using System.Text;
using Org.Apache.REEF.Common.Context;
using Org.Apache.REEF.Common.Events;
using Org.Apache.REEF.Common.Runtime.Evaluator.Context;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Implementations.Tang;
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
            var contextConfig = ContextConfiguration.ConfigurationModule
                .Set(ContextConfiguration.Identifier, "ID")
                .Set(ContextConfiguration.OnContextStart, GenericType<ContextEventHandler>.Class)
                .Set(ContextConfiguration.OnContextStop, GenericType<ContextEventHandler>.Class)
                .Set(ContextConfiguration.OnMessage, GenericType<ContextEventHandler>.Class)
                .Build();
            
            var injector = TangFactory.GetTang().NewInjector();

            var handler = new ContextEventHandler();
            injector.BindVolatileInstance(GenericType<ContextEventHandler>.Class, handler);

            using (var contextRuntime = new ContextRuntime(injector, contextConfig,
                    Optional<ContextRuntime>.Empty()))
            {
                contextRuntime.HandleContextMessage(Encoding.UTF8.GetBytes(hello));
            }

            Assert.True(handler.Started, "Handler did not receive the start signal.");
            Assert.True(handler.Stopped, "Handler did not receive the stop signal.");
            Assert.Equal(Encoding.UTF8.GetString(handler.MessageReceived), hello);
        }

        private sealed class ContextEventHandler 
            : IObserver<IContextStart>, IObserver<IContextStop>, IContextMessageHandler
        {
            [Inject]
            public ContextEventHandler()
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
    }
}
