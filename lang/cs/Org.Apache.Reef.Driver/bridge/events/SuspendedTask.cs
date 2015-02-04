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

using Org.Apache.Reef.Driver.Context;
using System;
using System.Runtime.Serialization;

namespace Org.Apache.Reef.Driver.Bridge
{
    [DataContract]
    internal class SuspendedTask : ISuspendedTask
    {
        internal SuspendedTask(ISuspendedTaskClr2Java suspendedTaskClr2Java)
        {
            InstanceId = Guid.NewGuid().ToString("N");
            SuspendedTaskClr2Java = suspendedTaskClr2Java;
            ActiveContextClr2Java = suspendedTaskClr2Java.GetActiveContext();
        }

        [DataMember]
        public string InstanceId { get; set; }

        public byte[] Message
        {
            get
            {
                return SuspendedTaskClr2Java.Get();
            }

            set
            {
            }
        }

        public string Id
        {
            get
            {
                return SuspendedTaskClr2Java.GetId();
            }

            set
            {
            }
        }

        public IActiveContext ActiveContext
        {
            get
            {
                return new ActiveContext(ActiveContextClr2Java);
            }

            set
            {
            }
        }

        [DataMember]
        private ISuspendedTaskClr2Java SuspendedTaskClr2Java { get; set; }

        [DataMember]
        private IActiveContextClr2Java ActiveContextClr2Java { get; set; }
    }
}
