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
using Org.Apache.REEF.Demo.Driver;
using Org.Apache.REEF.Driver;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Tang.Interface;

namespace Org.Apache.REEF.Demo.Examples
{
    public class DemoDriver : IObserver<IDriverStarted>
    {
        private readonly IDataSetMaster _dataSetMaster;
        private readonly Uri _dataSetUri;
        private IDataSet<SomeSerializableClass> _dataSet;

        [Inject]
        private DemoDriver(IDataSetMaster dataSetMaster,
                           [Parameter(typeof(DataSetUri))] string dataSetUriString)
        {
            _dataSetMaster = dataSetMaster;
            _dataSetUri = ToUri(dataSetUriString);
        }

        public void OnNext(IDriverStarted driverStarted)
        {
            // this will spawn evaluators loaded with blocks
            // the IDataSetMaster implementations will need to have OnNext(IAllocatedEvaluator) and OnNext(IActiveContext) handlers
            _dataSet = _dataSetMaster.Load<SomeSerializableClass>(_dataSetUri);

            // MiniDriver configuration
            // information on which block is on which evaluator will be given
            IConfiguration conf = TangFactory.GetTang().NewConfigurationBuilder().Build();
            IDataSet<AnotherSerializableClass> anotherDataSet = _dataSet.RunStage<AnotherSerializableClass>(conf);

            // store the new dataset/model
            _dataSetMaster.Store(anotherDataSet);
        }

        public void OnCompleted()
        {
        }

        public void OnError(Exception e)
        {
        }

        public static Uri ToUri(string uriString)
        {
            return new Uri(uriString);
        }
    }
}
