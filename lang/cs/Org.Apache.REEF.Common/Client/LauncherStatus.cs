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
using System.Text;
using Org.Apache.REEF.Utilities;

namespace Org.Apache.REEF.Common.Client
{
    public sealed class LauncherStatus
    {
        enum State
        {
            Init,
            Submitted,
            Running,
            Completed,
            Failed,
            ForceClosed
        }

        public static readonly LauncherStatus InitStatus = new LauncherStatus(State.Init);
        public static readonly LauncherStatus SubmittedStatus = new LauncherStatus(State.Submitted);
        public static readonly LauncherStatus RunningStatus = new LauncherStatus(State.Running);
        public static readonly LauncherStatus CompletedStatus = new LauncherStatus(State.Completed);
        public static readonly LauncherStatus ForceCloseStatus = new LauncherStatus(State.ForceClosed);
        public static readonly LauncherStatus FailedStatus = new LauncherStatus(State.Failed);

        private readonly State _state;

        public Optional<Exception> Error { get; }

        public bool IsDone
        {
            get
            {
                switch (_state)
                {
                    case State.Failed:
                    case State.Completed:
                    case State.ForceClosed:
                        return true;
                    default:
                        return false;
                }
            }
        }

        public bool IsSuccess => _state == State.Completed;

        public bool IsRunning => _state == State.Running;

        private LauncherStatus(State state) : this(state, null)
        {
        }

        private LauncherStatus(State state, Exception ex)
        {
            _state = state;
            Error = Optional<Exception>.OfNullable(ex);
        }

        public static LauncherStatus Failed(Exception ex)
        {
            return new LauncherStatus(State.Failed, ex);
        }

        public static LauncherStatus Failed(Optional<Exception> ex)
        {
            return new LauncherStatus(State.Failed, ex.OrElse(null));
        }
        public override bool Equals(Object other)
        {
            return this == other ||
                   other is LauncherStatus && (other as LauncherStatus)._state == _state;
        }

        public override int GetHashCode()
        {
            return _state.GetHashCode();
        }

        public override String ToString()
        {
            if (Error.IsPresent())
            {
                return _state + "(" + Error.Value + ")";
            }
            else
            {
                return _state.ToString();
            }
        }
    }
}
