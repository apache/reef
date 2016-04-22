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

namespace Org.Apache.REEF.IMRU.OnREEF.Driver.StateMachine
{
    /// <summary>
    /// State transition that represents the current state and event
    /// </summary>
    /// <typeparam name="TState"></typeparam>
    /// <typeparam name="TEvent"></typeparam>
    internal sealed class StateTransition<TState, TEvent> 
    {
        private readonly TState _currentState;
        private readonly TEvent _event;

        /// <summary>
        /// Create a state transition with current state and event
        /// </summary>
        /// <param name="currentState"></param>
        /// <param name="anEvent"></param>
        internal StateTransition(TState currentState, TEvent anEvent)
        {
            _currentState = currentState;
            _event = anEvent;
        }

        public override int GetHashCode()
        {
            return 17 + (31 * _currentState.GetHashCode()) + (31 * _event.GetHashCode());
        }

        public override bool Equals(object obj)
        {
            StateTransition<TState, TEvent> other = obj as StateTransition<TState, TEvent>;
            return other != null && _currentState.Equals(other._currentState) && _event.Equals(other._event);
        }
    }
}
