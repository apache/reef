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
using System.Globalization;
using System.Net;
using System.Threading;
using Org.Apache.REEF.Driver.Bridge;
using Org.Apache.REEF.Driver.Task;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Utilities;
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.Examples.AllHandlers
{
    /// <summary>
    /// A sample implementation of TaskCompletedHandler
    /// </summary>
    public sealed class HelloTaskCompletedHandler : IObserver<ICompletedTask>, IHttpHandler
    {
        private static readonly Logger Logger = Logger.GetLogger(typeof(HelloTaskCompletedHandler));

        private readonly IList<ICompletedTask> _completedTasks = new List<ICompletedTask>();

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
            Console.WriteLine("Received CompletedTask: {0}", completedTask.Id);
            _completedTasks.Add(completedTask);
        }

        public void OnError(Exception error)
        {
            throw new NotImplementedException();
        }

        public void OnCompleted()
        {
            throw new NotImplementedException();
        }

        public string GetSpecification()
        {
            return "CMD";
        }

        /// <summary>
        /// It gets HTTP request and close the contexts after receiving all the completed tasks
        /// </summary>
        /// <param name="requet"></param>
        /// <param name="resonse"></param>
        public void OnHttpRequest(ReefHttpRequest requet, ReefHttpResponse resonse)
        {
            var msg = string.Format(CultureInfo.CurrentCulture,
                      "HelloTaskCompletedHandler OnHttpRequest: URL: {0}, QueryString: {1}, inputStream: {2}.",
                      requet.Url, requet.Querystring, ByteUtilities.ByteArraysToString(requet.InputStream));
            Logger.Log(Level.Info, msg);
            resonse.Status = HttpStatusCode.OK;
            while (_completedTasks.Count != 2)
            {
                Thread.Sleep(1000);
                Logger.Log(Level.Info, "_completedTasks received:" + _completedTasks.Count);
            }
            resonse.OutputStream = ByteUtilities.StringToByteArrays("Stopped!!!");
            foreach (var t in _completedTasks)
            {
                t.ActiveContext.Dispose();
            }
            _completedTasks.Clear();
        }
    }
}