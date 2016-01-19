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
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Utilities;
using Org.Apache.REEF.Wake.Remote;

namespace Org.Apache.REEF.Examples.MachineLearning.KMeans.codecs
{
    /// <summary>
    /// TODO: use proper avro scheme to do encode/decode
    /// </summary>
    public class ProcessedResultsCodec : ICodec<ProcessedResults>
    {
        [Inject]
        public ProcessedResultsCodec()
        {
        }

        public byte[] Encode(ProcessedResults results)
        {
            return ByteUtilities.StringToByteArrays(results.Loss + "+" + string.Join("@", results.Means.Select(m => m.ToString())));
        }

        public ProcessedResults Decode(byte[] data)
        {
            string[] parts = ByteUtilities.ByteArraysToString(data).Split('+');
            if (parts.Count() != 2)
            {
                throw new ArgumentException("cannot deserialize from" + ByteUtilities.ByteArraysToString(data));
            }
            float loss = float.Parse(parts[0], CultureInfo.InvariantCulture);
            List<PartialMean> means = parts[1].Split('@').Select(PartialMean.FromString).ToList();
            return new ProcessedResults(means, loss);
        }
    }
}
