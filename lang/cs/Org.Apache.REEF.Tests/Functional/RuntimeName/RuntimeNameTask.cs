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
using Org.Apache.REEF.Common.Tasks;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.Tests.Functional.Messaging
{
    public sealed class RuntimeNameTask : ITask
    {
        public const string MessageSend = "MESSAGE:TASK";

        private static readonly Logger Logger = Logger.GetLogger(typeof(RuntimeNameTask));

        [Inject]
        public RuntimeNameTask()
        {
        }
        
        public byte[] Call(byte[] memento)
        {
            Logger.Log(Level.Info, "Hello, CLR Task!");
            return null;
        }

        public void Dispose()
        {
            Logger.Log(Level.Info, "Msg disposed.");
        }
    }
}