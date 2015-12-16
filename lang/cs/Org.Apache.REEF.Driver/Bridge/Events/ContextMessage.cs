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

using Org.Apache.REEF.Common.Context;
using Org.Apache.REEF.Driver.Bridge.Clr2java;

namespace Org.Apache.REEF.Driver.Bridge.Events
{
    internal sealed class ContextMessage : IContextMessage
    {
        private readonly string _messageSourcId;
        private readonly byte[] _bytes;
        private readonly string _id;

        public ContextMessage(IContextMessageClr2Java clr2Java)
        {
            _messageSourcId = clr2Java.GetMessageSourceId();
            _bytes = clr2Java.Get();
            _id = clr2Java.GetId();
        }

        public string Id
        {
            get { return _id; }
        }

        public string MessageSourceId
        {
            get { return _messageSourcId; }
        }

        public byte[] Message
        {
            get { return _bytes; }
        }
    }
}
