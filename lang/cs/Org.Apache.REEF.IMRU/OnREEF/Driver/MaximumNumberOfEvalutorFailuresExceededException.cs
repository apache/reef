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
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.IMRU.OnREEF.Driver
{
    /// <summary>
    /// Type of exception thrown when number of failed evaluators reach the 
    /// maximum allowed limit.
    /// </summary>
    public sealed class MaximumNumberOfEvaluatorFailuresExceededException : Exception
    {
        private static readonly Logger Logger =
            Logger.GetLogger(typeof(MaximumNumberOfEvaluatorFailuresExceededException));

        /// <summary>
        /// Constructor for throwing exception when the number of evaluator failures reaches maximum limit.
        /// </summary>
        /// <param name="maximumAllowedEvaluatorFailures">maximum number of evaluators allowed to fail</param>
        public MaximumNumberOfEvaluatorFailuresExceededException(int maximumAllowedEvaluatorFailures)
            : base(CreateMessage(maximumAllowedEvaluatorFailures))
        {
        }

        private static string CreateMessage(int maximumAllowedEvaluatorFailures)
        {
            return string.Format("Exiting IMRU. Number of failed evaluators reach the maximum allowed limit of {0}",
                maximumAllowedEvaluatorFailures);
        }
    }
}
