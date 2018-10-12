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
using Org.Apache.REEF.Common.Tasks;
using Org.Apache.REEF.Utilities.Logging;
using System;
using System.Threading;

namespace Org.Apache.REEF.Bridge.Core.Tests.Fail.Task
{
    internal abstract class BlockingCallTaskBase : ITask
    {
        private static readonly Logger Log = Logger.GetLogger(typeof(BlockingCallTaskBase));

        private bool _isRunning = true;

        public void Dispose()
        {
            lock (this)
            {
                _isRunning = false;
                Monitor.Pulse(this);
            }
        }

        public byte[] Call(byte[] memento)
        {
            lock (this)
            {
                Log.Log(Level.Info, "BlockingCallTaskBase.call() invoked. Waiting for the message.");
                while (_isRunning)
                {
                    try
                    {
                        Monitor.Wait(this);
                    }
                    catch (System.Exception ex)
                    {
                        Log.Log(Level.Warning, "wait() interrupted.", ex);
                    }
                }
            }
            return new byte[0];
        }

        public void OnError(System.Exception error)
        {
            Log.Log(Level.Error, "OnError called", error);
            throw error;
        }

        public void OnCompleted()
        {
            throw new NotSupportedException("OnComplete not supported by task");
        }
    }
}