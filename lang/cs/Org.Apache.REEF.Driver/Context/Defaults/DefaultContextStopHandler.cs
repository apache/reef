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
using Org.Apache.REEF.Common.Events;
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.Driver.Context.Defaults
{
    /// <summary>
    /// Default event handler for ContextStop
    /// </summary>
    [Obsolete("Deprecated in 0.14, please use Org.Apache.REEF.Common.Evaluator.Defaults.DefaultContextStopHandler instead.")]
    public class DefaultContextStopHandler : IObserver<IContextStop>
    {
        private static readonly Logger LOGGER = Logger.GetLogger(typeof(DefaultContextStopHandler));

        public void OnNext(IContextStop contextStop)
        {
            LOGGER.Log(Level.Info, "DefaultContextStopHandler received for context: " + contextStop.Id);
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
