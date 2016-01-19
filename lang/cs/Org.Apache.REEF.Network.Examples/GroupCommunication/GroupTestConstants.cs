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

namespace Org.Apache.REEF.Network.Examples.GroupCommunication
{
    public class GroupTestConstants
    {
        public const string DriverId = "BroadcastReduceDriver";
        public const string GroupName = "BroadcastReduceGroup";
        public const string BroadcastOperatorName = "Broadcast";
        public const string ReduceOperatorName = "Reduce";
        public const string ScatterOperatorName = "Scatter";
        public const string MasterTaskId = "MasterTask";
        public const string SlaveTaskId = "SlaveTask-";
        public const int NumIterations = 10;
        public const int FanOut = 2;
        public const int ChunkSize = 2;
        public const int ArrayLength = 6;
    }
}
