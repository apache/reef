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
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Utilities;

namespace Org.Apache.REEF.Common.Runtime.Evaluator.Context
{
    internal sealed class ContextClientCodeException : Exception
    {
        private readonly string _contextId;
        private readonly Optional<string> _parentId;

        /// <summary>
        /// construt the exception that caused the error
        /// </summary>
        /// <param name="contextId"> the id of the failed context.</param>
        /// <param name="parentId"> the id of the failed context's parent, if any.</param>
        /// <param name="message"> the error message </param>
        /// <param name="cause"> the exception that caused the error</param>
        public ContextClientCodeException(
                string contextId,
                Optional<string> parentId,
                string message,
                Exception cause)
            : base("Failure in context '" + contextId + "': " + message, cause)
        {
            _contextId = contextId;
            _parentId = parentId;
        }

        public string ContextId
        {
            get { return _contextId; }
        }

        public Optional<string> ParentId
        {
            get { return _parentId; }
        }

        /// <summary>
        /// Extracts a context id from the given configuration.
        /// </summary>
        /// <param name="c"></param>
        /// <returns>the context id in the given configuration.</returns>
        public static string GetId(IConfiguration c)
        {
            // TODO: update after TANG is available
            return string.Empty;
        }
    }
}
