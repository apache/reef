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

namespace Org.Apache.REEF.IMRU.OnREEF.Driver.StateMachine
{
    /// <summary>
    /// Exception for SystemState Transition
    /// </summary>
    internal sealed class SystemStateTransitionException : Exception
    {
        /// <summary>
        /// Exception when error happens in system state transition
        /// </summary>
        /// <param name="systemState"></param>
        /// <param name="stateEvent"></param>
        internal SystemStateTransitionException(SystemState systemState, SystemStateEvent stateEvent)
            : base(ExceptionMessage(systemState, stateEvent))
        {            
        }

        /// <summary>
        /// Format a message
        /// </summary>
        /// <param name="systemState"></param>
        /// <param name="stateEvent"></param>
        /// <returns></returns>
        private static string ExceptionMessage(SystemState systemState, SystemStateEvent stateEvent)
        {
            return string.Format(CultureInfo.InvariantCulture, "Unexpected event {0} in state {1}.", stateEvent, systemState);
        }
    }
}
