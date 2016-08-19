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
using Org.Apache.REEF.Common.Events;
using Org.Apache.REEF.Demo.Task;
using Org.Apache.REEF.IO.PartitionedData;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Formats;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.Demo.Driver
{
    /// <summary>
    /// Fetch partitions assigned to this evaluator, using the given SerializedInitialDataLoadPartitions object.
    /// </summary>
    internal sealed class DataSetLoadContext : IObserver<IContextStart>
    {
        private static readonly Logger Logger = Logger.GetLogger(typeof(DataSetLoadContext));

        [Inject]
        private DataSetLoadContext([Parameter(typeof(SerializedInitialDataLoadPartitions))] ISet<string> seralizedInitialDataLoadPartitions,
                                   [Parameter(typeof(LoadedDataSetIdNamedParameter))] string dataSetId,
                                   AvroConfigurationSerializer avroConfigurationSerializer,
                                   IInjector injector,
                                   DataSetManager dataSetManager)
        {
            foreach (var serializedInitialDataLoadPartition in seralizedInitialDataLoadPartitions)
            {
                var forkedInjector =
                    injector.ForkInjector(avroConfigurationSerializer.FromString(serializedInitialDataLoadPartition));
                var inputPartition = forkedInjector.GetInstance<IInputPartition<byte[]>>();

                dataSetManager.AddLocalPartition(dataSetId, inputPartition, false);
            }
        }

        public void OnNext(IContextStart value)
        {
        }

        public void OnError(Exception error)
        {
            throw error;
        }

        public void OnCompleted()
        {
        }
    }
}
