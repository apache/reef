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
using Org.Apache.REEF.Utilities.Attributes;

namespace Org.Apache.REEF.Client.YARN
{
    /// <summary>
    /// This interface represents application information maintained by YARN RM.
    /// This interface is modeled on Org.Apache.REEF.Client.YARN.RestClient.DataModel.Application. 
    /// Documentation on the class Org.Apache.REEF.Client.YARN.RestClient.DataModel.Application
    /// can be found here.
    /// <a href="http://hadoop.apache.org/docs/r2.6.0/hadoop-yarn/hadoop-yarn-site/WebServicesIntro.html">
    /// Hadoop RM REST API</a> documentation.
    /// </summary>
    [Unstable("0.17", "Working in progress. For local runtime, some of the property, such as FinalState and AppId are not implemented yet.")]
    public interface IApplicationReport
    {
        /// <summary>
        /// Get YARN application name.
        /// </summary>
        string AppName { get; }
        
        /// <summary>
        /// Get YARN application start time.
        /// </summary>
        long StartedTime { get; }

        /// <summary>
        /// Get YARN application finish time.
        /// </summary>
        long FinishedTime { get; }

        /// <summary>
        /// Get YARN application number of running containers.
        /// </summary>
        int NumberOfRunningEvaluators { get; }

        /// <summary>
        /// This method returns the url of http server running inside the driver.
        /// e.g. http://hostname:port/
        /// </summary>
        string Url { get; }

        /// <summary>
        /// Get YARN application id.
        /// </summary>
        string AppId { get; }

        /// <summary>
        /// Get Application final state.
        /// </summary>
        FinalState FinalState { get; }
    }
}
