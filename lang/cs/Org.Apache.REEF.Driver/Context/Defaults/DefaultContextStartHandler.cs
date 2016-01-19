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
    /// Default handler for ContextStart
   /// </summary>
    public class DefaultContextStartHandler : IObserver<IContextStart>
    {
        private static readonly Logger LOGGER = Logger.GetLogger(typeof(DefaultContextStartHandler));

        public void OnNext(IContextStart contextStart)
        {
            LOGGER.Log(Level.Info, "DefaultContextStartHandler received for context: " + contextStart.Id);
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
