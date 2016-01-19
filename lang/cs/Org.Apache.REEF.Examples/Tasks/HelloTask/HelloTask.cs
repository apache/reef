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
using System.Linq;
using System.Net;
using System.Threading;
using Org.Apache.REEF.Common.Io;
using Org.Apache.REEF.Common.Tasks;
using Org.Apache.REEF.Common.Tasks.Events;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Utilities;
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.Examples.Tasks.HelloTask
{
    public class HelloTask : ITask
    {
        private static readonly Logger LOGGER = Logger.GetLogger(typeof(HelloTask));

        private readonly INameClient _nameClient = null;

        [Inject]
        public HelloTask()
        {
            Console.WriteLine("HelloTask constructor 0");
        }

        [Inject]
        public HelloTask(HelloService service, INameClient nameClient)
        {
            Console.WriteLine("HelloTask constructor 2");
            Service = service;
            _nameClient = nameClient;
        }

        [Inject]
        public HelloTask(HelloService service)
        {
            Console.WriteLine("HelloTask constructor 1");
            Service = service;
        }

        public HelloService Service { get; set; }

        public byte[] Call(byte[] memento)
        {
            Console.WriteLine("Hello, CLR REEF!");
            if (_nameClient != null)
            {
                _nameClient.Register("abc", new IPEndPoint(IPAddress.Any, 8080));
                Console.WriteLine("IP Address: {0}", _nameClient.Lookup("abc"));
            }
            PrintGuestList();
            Thread.Sleep(5 * 1000);
            Console.WriteLine("Bye, CLR REEF!");

            return null;
        }

        public void Dispose()
        {
            LOGGER.Log(Level.Info, "Hello task disposed.");
        }

        private void HandleDriverMessage(string message)
        {
            using (LOGGER.LogFunction("HelloTask::HandleDriverMessage"))
            {
                LOGGER.Log(Level.Info, "I handle message by logging : " + message);
            }
        }

        private void PrintGuestList()
        {
            if (Service == null || !Service.Guests.Any())
            {
                Console.WriteLine("No service provided.");
            }
            else
            {
                Console.WriteLine("Serving guest: " + string.Join(";", Service.Guests));
            }
        }

        public class HelloDriverMessageHandler : IDriverMessageHandler
        {
            private readonly HelloTask _parentTask;

            [Inject]
            public HelloDriverMessageHandler(HelloTask task)
            {
                _parentTask = task;
            }

            public void Handle(IDriverMessage value)
            {
                string message = string.Empty;
                LOGGER.Log(Level.Verbose, "Receieved a message from driver, handling it with HelloDriverMessageHandler");
                if (value.Message.IsPresent())
                {
                    message = ByteUtilities.ByteArraysToString(value.Message.Value);
                }
                _parentTask.HandleDriverMessage(message);
            }
        }
    }
}
