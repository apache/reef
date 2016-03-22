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
using Org.Apache.REEF.Common.Events;
using Org.Apache.REEF.IO.PartitionedData;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Utilities.Diagnostics;
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.IMRU.OnREEF.Driver
{
    internal class DataLoadingContext<T> : IObserver<IContextStart>
    {
        private static readonly Logger Logger = Logger.GetLogger(typeof(DataLoadingContext<T>));

        [Inject]
        internal DataLoadingContext(IInputPartition<T> partition)
        {
            Logger.Log(Level.Info, "Entered data loading context");
        }

        public void OnNext(IContextStart value)
        {
        }

        public void OnError(Exception error)
        {
            Exceptions.Throw(error, "Error occured in Data Loading context start", Logger);
        }

        public void OnCompleted()
        {
        }
    }
}
