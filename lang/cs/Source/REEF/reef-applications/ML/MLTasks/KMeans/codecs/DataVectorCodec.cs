/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Org.Apache.Reef.Applications.MLTasks.KMeans.Contracts;
using Org.Apache.Reef.Utilities;
using Org.Apache.Reef.Tang.Annotations;
using Org.Apache.Reef.Wake.Remote;

namespace Org.Apache.Reef.Applications.MLTasks.KMeans.codecs
{
    public class DataVectorCodec : ICodec<DataVector>
    {
        [Inject]
        public DataVectorCodec()
        {
        }

        public byte[] Encode(DataVector obj)
        {
            DataVectorContract contract = DataVectorContract.Create(obj);
            return AvroUtils.AvroSerialize(contract);
        }

        public DataVector Decode(byte[] data)
        {
            DataVectorContract contract = AvroUtils.AvroDeserialize<DataVectorContract>(data);
            return contract.ToDataVector();
        }
    }
}
