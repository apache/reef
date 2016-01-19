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
using System.Globalization;
using Org.Apache.REEF.Driver.Task;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Utilities;

namespace Org.Apache.REEF.Examples.AllHandlers
{
    /// <summary>
    /// A sample implementation of FailedTask Handler
    /// </summary>
    public class HelloFailedTaskHandler : IObserver<IFailedTask>
    {
        [Inject]
        private HelloFailedTaskHandler()
        {
        }

        /// <summary>
        /// Sample code close the active context when task is failed
        /// </summary>
        /// <param name="failedTask"></param>
        public void OnNext(IFailedTask failedTask)
        {
            string errorMessage = string.Format(
                CultureInfo.InvariantCulture,
                "Task [{0}] has failed caused by [{1}], with message [{2}] and description [{3}]. The raw data for failure is [{4}].",
                failedTask.Id,
                failedTask.Reason.IsPresent() ? failedTask.Reason.Value : string.Empty,
                failedTask.Message,
                failedTask.Description.IsPresent() ? failedTask.Description.Value : string.Empty,
                failedTask.Data.IsPresent() ? ByteUtilities.ByteArraysToString(failedTask.Data.Value) : string.Empty);

            Console.WriteLine(errorMessage);

            if (failedTask.GetActiveContext().IsPresent())
            {
                Console.WriteLine("Disposing the active context the failed task ran in.");

                // we must do something here: either close the context or resubmit a task to the active context
                failedTask.GetActiveContext().Value.Dispose();
            }
        }

        public void OnCompleted()
        {
            throw new NotImplementedException();
        }

        public void OnError(Exception error)
        {
            throw new NotImplementedException();
        }
    }
}
