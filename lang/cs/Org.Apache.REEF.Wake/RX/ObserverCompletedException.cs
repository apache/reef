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

namespace Org.Apache.REEF.Wake.RX
{
    /// <summary>
    /// It is illegal to call onError() or onCompleted() when a call to onNext() is
    /// still outstanding, or to call onNext(), onError() or onCompleted() after a
    /// call to onError() or onCompleted() has been dispatched.
    /// </summary>
    /// <remarks>
    /// It is illegal to call onError() or onCompleted() when a call to onNext() is
    /// still outstanding, or to call onNext(), onError() or onCompleted() after a
    /// call to onError() or onCompleted() has been dispatched. Observers may throw
    /// an ObserverCompleted exception whenever this API is violated. Violating the
    /// API leaves the Observer (and any resources that it holds) in an undefined
    /// state, and throwing ObserverCompleted exceptions is optional.
    /// Callers receiving this exception should simply pass it up the stack to the
    /// Aura runtime. They should not attempt to forward it on to upstream or
    /// downstream stages. The easiest way to do this is to ignore the exception
    /// entirely.
    /// </remarks>
    [System.Serializable]
    public sealed class ObserverCompletedException : InvalidOperationException
    {
        private const long serialVersionUID = 1L;
    }
}
