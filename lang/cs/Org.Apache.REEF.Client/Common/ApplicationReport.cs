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

using Org.Apache.REEF.Client.YARN.RestClient.DataModel;

namespace Org.Apache.REEF.Client.Common
{
    public class ApplicationReport : IApplicationReport
    {
        public string AppName { get; }
        public long StartedTime { get; }
        public long FinishedTime { get; }
        public int RunningContainers { get; }
        public string DriverUrl { get; }
        public string AppId { get; }
        public FinalState FinalState { get; }

        internal ApplicationReport(string appId, string appName, string driverUrl, 
            long startedTime, long finishedTime, int runningContainers, FinalState finalState)
        {
            AppId = appId;
            AppName = appName;
            DriverUrl = driverUrl;
            FinalState = finalState;
            StartedTime = startedTime;
            FinishedTime = finishedTime;
            RunningContainers = runningContainers;
        }
    }
}
