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
using Org.Apache.REEF.Common;

namespace Org.Apache.REEF.Driver.Context
{
    public interface IActiveContext : IDisposable, IContext, ITaskSubmittable, IContextSubmittable
    {
        /// <summary>
        /// Sends a context message to the Driver. Currently does not work due to the
        /// original .NET evaluator implementation not propagating ContextConfiguration
        /// properly via Tang. Work on the new Evaluator is being done in 
        /// <a href="https://issues.apache.org/jira/browse/REEF-289">REEF-289</a>.
        /// </summary>
        /// <param name="message">Message to send</param>
        /// TODO[JIRA REEF-863]: Test after unblocked by REEF-289
        void SendMessage(byte[] message); 
    }
}
