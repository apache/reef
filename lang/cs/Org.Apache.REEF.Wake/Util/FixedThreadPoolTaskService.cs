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
using System.Threading;
using System.Threading.Tasks;
using Org.Apache.REEF.Utilities.Diagnostics;
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.Wake.Util
{
    public sealed class FixedThreadPoolTaskService : ITaskService
    {
        private static readonly Logger LOGGER = Logger.GetLogger(typeof(FixedThreadPoolTaskService));

        private readonly TaskFactory factory;

        private readonly List<Task> tasks = new List<Task>();
        private bool shuttingDown;

        internal FixedThreadPoolTaskService(int maxDegreeOfParallelism)
        {
            LimitedConcurrencyLevelTaskScheduler lcts = new LimitedConcurrencyLevelTaskScheduler(maxDegreeOfParallelism);
            factory = new TaskFactory(lcts);
        }

        public bool AwaitTermination(long n, TimeSpan unit)
        {
            Task[] allTasks;
            lock (tasks)
            {
                if (tasks.Count == 0)
                {
                    return true;
                }
                allTasks = tasks.ToArray();
            }
            return Task.WaitAll(allTasks, unit);
        }

        public void ShutdownNow()
        {
            Shutdown();
        }

        public void Shutdown()
        {
            lock (tasks)
            {
                shuttingDown = true;
            }
        }

        public Task<T> Submit<T>(Func<T> c)
        {
            Task<T> task = null;
            lock (tasks)
            {                
                if (shuttingDown)
                {
                    Exceptions.Throw(new InvalidOperationException("Shutting down"), LOGGER);
                }

                CancellationTokenSource cancellationTokenSource = new CancellationTokenSource();
                CancellationToken cancellationToken = cancellationTokenSource.Token;
                task = factory.StartNew(c, cancellationToken);                                
                tasks.Add(task);
            }
            return task;
        }

        public void Execute(ThreadStart threadStart)
        {
            new Actionable(threadStart).Call();
        }

        internal void RemoveTask(Task task)
        {
            lock (tasks)
            {
                tasks.Remove(task);
            }
        }
    }
}
