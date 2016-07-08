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
using Org.Apache.REEF.Demo.Stage;
using Org.Apache.REEF.Driver.Context;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Tang.Interface;

namespace Org.Apache.REEF.Demo.Driver
{
    public class DataSet<T> : IDataSet<T>
    {
        private readonly string _id;
        private readonly IDictionary<IActiveContext, ISet<BlockInfo>> _blockDistribution;

        public DataSet(string id,
                       IDictionary<IActiveContext, ISet<BlockInfo>> blockDistribution)
        {
            _id = id;
            _blockDistribution = blockDistribution;
        }

        public string Id
        {
            get { return _id; }
        }

        public IDataSet<T2> TransformPartitions<T2>(IConfiguration transformConf)
        {
            throw new NotImplementedException();
        }

        public IDataSet<T2> RunStage<T2>(IConfiguration stageConf)
        {
            MiniDriver miniDriver = TangFactory.GetTang().NewInjector(stageConf).GetInstance<MiniDriver>();
            miniDriver.OnStart(_blockDistribution);
            return null; // retrieve IDataSet somehow
        }

        public T[] Collect()
        {
            throw new NotImplementedException();
        }
    }
}
