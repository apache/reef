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

namespace Org.Apache.REEF.Wake.RX
{
    /// <summary>
    /// An observer with logging-only onError and onCompleted() methods.
    /// </summary>
    /// <typeparam name="T">The observer type</typeparam>
    public abstract class AbstractObserver<T> : IObserver<T>
    {
        private static readonly Logger LOGGER = Logger.GetLogger(typeof(AbstractObserver<T>));

        public virtual void OnError(Exception error)
        {
            LOGGER.Log(Level.Info, "The observer " + GetType() + "has received an Exception: " + error);
        }

        public virtual void OnCompleted()
        {
            LOGGER.Log(Level.Verbose, "The observer " + GetType() + "has received an onCompleted() ");
        }

        public abstract void OnNext(T arg1);
    }
}
