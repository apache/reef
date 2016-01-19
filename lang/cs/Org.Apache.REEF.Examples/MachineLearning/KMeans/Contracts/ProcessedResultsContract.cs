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

using System.Collections.Generic;
using System.Linq;
using System.Runtime.Serialization;

namespace Org.Apache.REEF.Examples.MachineLearning.KMeans.Contracts
{
    [DataContract]
    public class ProcessedResultsContract
    {
        [DataMember]
        public List<PartialMeanContract> PartialMeanContracts { get; set; }

        [DataMember]
        public float Loss { get; set; }

        public static ProcessedResultsContract Create(ProcessedResults obj)
        {
            List<PartialMeanContract> partialMeansContracts = obj.Means
                .Select(PartialMeanContract.Create)
                .ToList();

            return new ProcessedResultsContract
            {
                PartialMeanContracts = partialMeansContracts,
                Loss = obj.Loss
            };
        }

        public ProcessedResults ToProcessedResults()
        {
            List<PartialMean> partialMeans = PartialMeanContracts
                .Select(contract => contract.ToPartialMean())
                .ToList();

            return new ProcessedResults(partialMeans, Loss);
        }
    }
}
