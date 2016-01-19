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
using Org.Apache.REEF.Driver.Context;
using Org.Apache.REEF.Driver.Task;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.Driver.Defaults
{
    /// <summary>
    /// Default event handler for CompletedTask: Log it and close the context.
    /// </summary>
    public class DefaultTaskCompletionHandler : IObserver<ICompletedTask>
    {
        private static readonly Logger LOGGER = Logger.GetLogger(typeof(DefaultTaskCompletionHandler));
        
        [Inject]
        public DefaultTaskCompletionHandler()
        {
        }

        public void OnNext(ICompletedTask value)
        {
            IActiveContext activeContext = value.ActiveContext;
            LOGGER.Log(Level.Info, string.Format(CultureInfo.InvariantCulture, "Received CompletedTask: {0} :: CLOSING context: {1}", value.Id, activeContext.Id));
            activeContext.Dispose();
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
