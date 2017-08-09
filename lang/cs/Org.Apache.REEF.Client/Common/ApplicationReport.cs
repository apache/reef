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
    /// <summary>
    /// This class represents application information mainated by YARN RM.
    /// This class is modeled on Org.Apache.REEF.Client.YARN.RestClient.DataModel.Application. 
    /// Documentation on the class Org.Apache.REEF.Client.YARN.RestClient.DataModel.Application
    /// can be found here.
    /// <a href="http://hadoop.apache.org/docs/r2.6.0/hadoop-yarn/hadoop-yarn-site/WebServicesIntro.html">
    /// Hadoop RM REST API</a> documentation.
    /// </summary>
    internal sealed class ApplicationReport : IApplicationReport
    {
        public string AppName { get; private set; }
        public long StartedTime { get; private set; }
        public long FinishedTime { get; private set; }
        public int NumberOfRunningEvaluators { get; private set; }
        public string TrackingUrl { get; private set; }
        public string AppId { get; private set; }
        public FinalState FinalState { get; private set; }

        internal ApplicationReport(string appId, string appName, string trackingUrl, 
            long startedTime, long finishedTime, int numberOfRunningEvaluators, FinalState finalState)
        {
            AppId = appId;
            AppName = appName;
            TrackingUrl = trackingUrl;
            FinalState = finalState;
            StartedTime = startedTime;
            FinishedTime = finishedTime;
            NumberOfRunningEvaluators = numberOfRunningEvaluators;
        }

        public override string ToString()
        {
            return string.Format("AppName: {0} StartedTime: {1}	FinishedTime: {2}	" +
                                 "NumberOfRunningEvaluators: {3} TrackingUrl: {4} AppId: {5} FinalState: {6}",
                AppName,
                StartedTime,
                FinishedTime,
                NumberOfRunningEvaluators,
                TrackingUrl,
                AppId,
                FinalState);
        }
    }
}
