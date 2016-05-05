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

using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Utilities.Attributes;
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.Common.Runtime.Evaluator
{
    /// <summary>
    /// A logger that logs <see cref="Common.Constants.EvaluatorExitSuccessLog"/> on
    /// an Evaluator's successful exit and <see cref="Common.Constants.EvaluatorExitFailureLog"/>
    /// an Evaluator's failure. Will only log once.
    /// </summary>
    [ThreadSafe]
    internal sealed class EvaluatorExitLogger
    {
        private static readonly Logger Logger = Logger.GetLogger(typeof(EvaluatorExitLogger));

        private readonly object _lock = new object();
        private bool _exitLogged = false;

        [Inject]
        private EvaluatorExitLogger()
        {
        }

        public void LogExit(bool successfulExit)
        {
            lock (_lock)
            {
                if (_exitLogged)
                {
                    return;
                }

                if (successfulExit)
                {
                    Logger.Log(Level.Info, Common.Constants.EvaluatorExitSuccessLog);
                }
                else
                {
                    Logger.Log(Level.Error, Common.Constants.EvaluatorExitFailureLog);
                }

                _exitLogged = true;
            }
        }
    }
}