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
using System.Globalization;
using Org.Apache.Reef.Driver.Context;
using Org.Apache.Reef.Driver.Task;
using Org.Apache.Reef.Utilities;
using Org.Apache.Reef.Tang.Annotations;

namespace Org.Apache.Reef.Examples.HelloCLRBridge.Handlers
{
    /// <summary>
    /// Sample implementaion of RunningTaskHandler
    /// </summary>
    public class HelloDriverRestartRunningTaskHandler : IObserver<IRunningTask>
    {
        [Inject]
        public HelloDriverRestartRunningTaskHandler()
        {
        }

        public void OnNext(IRunningTask runningTask)
        {
            IActiveContext context = runningTask.ActiveContext;

            Console.WriteLine(string.Format(
                CultureInfo.InvariantCulture,
                "HelloDriverRestartRunningTaskHandler: Task [{0}] is running after driver restart. Evaluator id: [{1}].",
                runningTask.Id,
                context.EvaluatorId));

            runningTask.Send(ByteUtilities.StringToByteArrays(
                string.Format(
                CultureInfo.InvariantCulture,
                "Hello, task {0}! Glad to know that you are still running in Evaluator {1} after driver restart!",
                runningTask.Id,
                context.EvaluatorId)));
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
