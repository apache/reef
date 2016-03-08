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

using System.Threading.Tasks;
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.Utilities.AsyncUtils
{
    /// <summary>
    /// Logging helpers for TPL
    /// </summary>
    public static class LoggingHelper
    {
        /// <summary>
        /// Logs the and then ignores the exceptions thrown from the task if any.
        /// </summary>
        /// <param name="self">The task itself</param>
        /// <param name="logger">Logger to log against</param>
        /// <param name="msg">Optional message to be included</param>
        /// <param name="logLevel">Optional parameter to set log level</param>
        public static void LogAndIgnoreExceptionIfAny(this Task self, Logger logger, string msg = "", Level logLevel = Level.Error)
        {
            self.ContinueWith(t =>
            {
                // ReSharper disable once PossibleNullReferenceException ; We know the task is Faulted
                logger.Log(logLevel, "{0} Exception:{1}", t.Exception.GetBaseException());
            },
                TaskContinuationOptions.OnlyOnFaulted | TaskContinuationOptions.ExecuteSynchronously);
        }
    }
}