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

namespace Org.Apache.REEF.IMRU.OnREEF.MapInputWithControlMessage
{
    /// <summary>
    /// Input to Map task
    /// Containes both actual message of type TMapInput and control 
    /// message from UpdateTask
    /// </summary>
    /// <typeparam name="TMapInput"></typeparam>
    internal sealed class MapInputWithControlMessage<TMapInput> : IDisposable
    {
        /// <summary>
        /// Internal constructor
        /// </summary>
        /// <param name="controlMessage">Control message from Update Function</param>
        internal MapInputWithControlMessage(MapControlMessage controlMessage)
        {
            ControlMessage = controlMessage;
        }

        /// <summary>
        /// Internal constructor
        /// </summary>
        /// <param name="input">Actual map input</param>
        /// <param name="controlMessage">Control message from Update Function</param>
        internal MapInputWithControlMessage(TMapInput input, MapControlMessage controlMessage)
        {
            Message = input;
            ControlMessage = controlMessage;
        }

        /// <summary>
        /// Actual input for Mappers
        /// </summary>
        internal TMapInput Message { get; set; }

        /// <summary>
        /// Control message from Update Task to Map task
        /// </summary>
        internal MapControlMessage ControlMessage { get; set; }

        public void Dispose()
        {
        }
    }
}
