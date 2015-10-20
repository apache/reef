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
using Org.Apache.REEF.Tang.Annotations;

namespace Org.Apache.REEF.IMRU.OnREEF.ResultHandler
{
    internal class DefaultResultHandler<TResult> : IObserver<TResult>
    {
        [Inject]
        private DefaultResultHandler()
        {
        }

        /// <summary>
        /// Specifies how to handle the IMRU results from UpdateTask. Does nothing
        /// </summary>
        /// <param name="value">The result of IMRU</param>
        public void OnNext(TResult value)
        {
        }

        /// <summary>
        ///  Handles error scenario. Just throw it in this case
        /// </summary>
        /// <param name="error">Exception</param>
        public void OnError(Exception error)
        {
            throw error;
        }

        /// <summary>
        /// Handles what to do on completion
        /// In this case do nothing
        /// </summary>
        public void OnCompleted()
        {
        }
    }
}
