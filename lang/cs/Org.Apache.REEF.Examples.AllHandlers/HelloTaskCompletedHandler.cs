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
using Org.Apache.REEF.Driver.Task;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Utilities;

namespace Org.Apache.REEF.Examples.AllHandlers
{
    /// <summary>
    /// A sample implementation of TaskCompletedHandler
    /// </summary>
    public sealed class HelloTaskCompletedHandler : IObserver<ICompletedTask>
    {
        [Inject]
        private HelloTaskCompletedHandler()
        {
        }

        /// <summary>
        /// Sample code to print out a completed task's details.
        /// </summary>
        /// <param name="completedTask"></param>
        public void OnNext(ICompletedTask completedTask)
        {
            Console.WriteLine("Received CompletedTask: {0}, with message [{1}].", completedTask.Id, ByteUtilities.ByteArraysToString(completedTask.Message));
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