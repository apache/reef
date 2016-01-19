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

using Org.Apache.REEF.Examples.MachineLearning.KMeans.Contracts;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Utilities;
using Org.Apache.REEF.Wake.Remote;

namespace Org.Apache.REEF.Examples.MachineLearning.KMeans.codecs
{
    /// <summary>
    /// Used to serialize and deserialize Centroids objects.
    /// </summary>
    public class CentroidsCodec : ICodec<Centroids>
    {
        [Inject]
        public CentroidsCodec()
        {
        }
        
        public byte[] Encode(Centroids centroids)
        {
            CentroidsContract contract = CentroidsContract.Create(centroids);
            return AvroUtils.AvroSerialize(contract);
        }

        public Centroids Decode(byte[] data)
        {
            CentroidsContract contract = AvroUtils.AvroDeserialize<CentroidsContract>(data);
            return contract.ToCentroids();
        }
    }
}
