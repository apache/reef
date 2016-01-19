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
using Org.Apache.REEF.Examples.MachineLearning.KMeans.codecs;
using Org.Apache.REEF.Utilities;

namespace Org.Apache.REEF.Examples.MachineLearning.KMeans
{
    /// <summary>
    /// ProcessedResults includes a list of "PartialMeans" and "Partial loss", but it can be used to denote
    /// the "whole" means as well, aggregated from all PartialMeans
    /// </summary>
    public class ProcessedResults
    {
        public ProcessedResults(List<PartialMean> means, float loss)
        {
            Means = means;
            Loss = loss;
        }

        public List<PartialMean> Means { get; set; }

        /// <summary>
        /// the loss for current slice computed from 
        /// </summary>
        public float Loss { get; set; }

        /// <summary>
        /// helper function mostly used for logging
        /// </summary>
        /// <returns>seralized string</returns>
        public override string ToString()
        {
            return ByteUtilities.ByteArraysToString(new ProcessedResultsCodec().Encode(this));
        }
    }
}
