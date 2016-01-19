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

namespace Org.Apache.REEF.Examples.MachineLearning.KMeans
{
    public class Constants
    {
        public const string KMeansExecutionBaseDirectory = @"KMeans";
        public const string DataDirectory = "data";
        public const string PartialMeanFilePrefix = "partialMeans_";
        public const string CentroidsFile = "centroids";
        public const string MasterTaskId = "KMeansMasterTaskId";
        public const string SlaveTaskIdPrefix = "KMeansSlaveTask_";
        public const string KMeansCommunicationGroupName = "KMeansBroadcastReduceGroup";
        public const string CentroidsBroadcastOperatorName = "CentroidsBroadcast";
        public const string ControlMessageBroadcastOperatorName = "ControlMessageBroadcast";
        public const string MeansReduceOperatorName = "MeansReduce";
    }
}
