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
using Org.Apache.REEF.Client.YARN.RestClient.DataModel;
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.Client.YARN
{
    /// <summary>
    /// This class represents application information maintained by YARN RM.
    /// This class is modeled on Org.Apache.REEF.Client.YARN.RestClient.DataModel.Application. 
    /// Documentation on the class Org.Apache.REEF.Client.YARN.RestClient.DataModel.Application
    /// can be found here.
    /// <a href="http://hadoop.apache.org/docs/r2.6.0/hadoop-yarn/hadoop-yarn-site/WebServicesIntro.html">
    /// Hadoop RM REST API</a> documentation.
    /// </summary>
    internal sealed class ApplicationReport : IApplicationReport
    {
        /// <summary>
        /// Application name.
        /// </summary>
        public string AppName { get; private set; }

        /// <summary>
        /// Application start time.
        /// </summary>
        public long StartedTime { get; private set; }

        /// <summary>
        /// Application finished time.
        /// </summary>
        public long FinishedTime { get; private set; }

        /// <summary>
        /// Number of running evaluators
        /// </summary>
        public int NumberOfRunningEvaluators { get; private set; }

        /// <summary>
        /// Driver uri
        /// </summary>
        public Uri TrackingUrl { get; private set; }

        /// <summary>
        /// Application id
        /// </summary>
        public string AppId { get; private set; }

        /// <summary>
        /// Application final state
        /// </summary>
        public FinalState FinalState { get; private set; }

        /// <summary>
        /// Application report constructor
        /// </summary>
        internal ApplicationReport(string appId, string appName, string trackingUrl, 
            long startedTime, long finishedTime, int numberOfRunningEvaluators, FinalState finalState)
        {
            AppId = appId;
            AppName = appName;
            TrackingUrl = trackingUrl == null ? null : new Uri(trackingUrl);
            FinalState = finalState;
            StartedTime = startedTime;
            FinishedTime = finishedTime;
            NumberOfRunningEvaluators = numberOfRunningEvaluators;
        }

        /// <summary>
        /// To string for printing/log
        /// </summary>
        /// <returns></returns>
        public override string ToString()
        {
            return string.Format("AppName: {0} StartedTime: {1}, FinishedTime: {2}," +
                                    "NumberOfRunningEvaluators: {3}, TrackingUrl: {4}, AppId: {5}, FinalState: {6}",
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
