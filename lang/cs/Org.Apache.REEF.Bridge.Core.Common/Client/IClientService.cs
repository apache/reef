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

namespace Org.Apache.REEF.Bridge.Core.Common.Client
{
    /// <summary>
    /// Interface that all client services implement that provide
    /// control flow channels to the driver.
    /// </summary>
    internal interface IClientService
    {
        /// <summary>
        /// Shutdown the job with an optional message.
        /// </summary>
        /// <param name="message">to deliver to the driver</param>
        void Close(byte[] message = null);

        /// <summary>
        /// Send the driver a message.
        /// </summary>
        /// <param name="message">to deliver to the driver</param>
        void Send(byte[] message);
    }
}
