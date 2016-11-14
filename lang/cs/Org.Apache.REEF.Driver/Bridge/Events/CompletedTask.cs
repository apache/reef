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

using System.Runtime.Serialization;
using Org.Apache.REEF.Driver.Bridge.Clr2java;
using Org.Apache.REEF.Driver.Context;
using Org.Apache.REEF.Driver.Task;

namespace Org.Apache.REEF.Driver.Bridge.Events
{
    [DataContract]
    internal class CompletedTask : ICompletedTask
    {
        internal CompletedTask(ICompletedTaskClr2Java completedTaskClr2Java)
        {
            CompletedTaskClr2Java = completedTaskClr2Java;
            ActiveContextClr2Java = completedTaskClr2Java.GetActiveContext();
        }

        public byte[] Message
        {
            get
            {
                return CompletedTaskClr2Java.Get();
            }
        }

        public string Id
        {
            get
            {
                return CompletedTaskClr2Java.GetId();
            }
        }

        public IActiveContext ActiveContext
        {
            get
            {
                return new ActiveContext(ActiveContextClr2Java);
            }
        }

        [DataMember]
        private ICompletedTaskClr2Java CompletedTaskClr2Java { get; set; }

        [DataMember]
        private IActiveContextClr2Java ActiveContextClr2Java { get; set; }
    }
}
