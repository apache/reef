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

using Org.Apache.Reef.Common.Api;
using Org.Apache.Reef.Common.ProtoBuf.ReefServiceProto;
using Org.Apache.Reef.Utilities;
using System;

namespace Org.Apache.Reef.Common
{
    public class FailedRuntime : AbstractFailure
    {
        public FailedRuntime(RuntimeErrorProto error)
            : base(error.identifier, error.message, null, GetException(error), error.exception)
        {
        }

        /// <summary>
        /// Get the exception from error
        /// </summary>
        /// <param name="error"></param>
        /// <returns>excetpiont from error</returns>
        private static Exception GetException(RuntimeErrorProto error)
        {
            byte[] data = error.exception;
            if (data != null)
            {
                return new InvalidOperationException(ByteUtilities.ByteArrarysToString(error.exception));
            }
            return null;
        }
    }
}
