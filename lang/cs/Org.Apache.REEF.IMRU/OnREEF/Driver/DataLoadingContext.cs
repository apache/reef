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
using Org.Apache.REEF.IO.PartitionedData;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Utilities.Diagnostics;
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.IMRU.OnREEF.Driver
{
    /// <summary>
    /// This is part of Root context of map evaluators and instantiates 
    /// input partition and later will call AddCache function also once
    /// REEF-1339 is resolved.
    /// </summary>
    /// <typeparam name="T">Data Handle Type</typeparam>
    internal class DataLoadingContext<T> : IObserver<IContextStart>
    {
        private static readonly Logger Logger = Logger.GetLogger(typeof(DataLoadingContext<T>));
        private readonly IInputPartition<T> _partition;

        [Inject]
        private DataLoadingContext(IInputPartition<T> partition)
        {
            _partition = partition;
            Logger.Log(Level.Verbose, "Entered data loading context");
        }

        /// <summary>
        /// Download data files when context starts.
        /// </summary>
        /// <param name="value">context start token</param>
        public void OnNext(IContextStart value)
        {
            _partition.Cache();
            Logger.Log(Level.Info, "Returned from IInputPartition.Cache().");
        }

        /// <summary>
        /// Specifies what to do if error occurs. We throw 
        /// the caught exception in this case.
        /// </summary>
        /// <param name="error">Exception</param>
        public void OnError(Exception error)
        {
            Exceptions.Throw(error, "Error occurred in Data Loading context start", Logger);
        }

        /// <summary>
        /// Specifies what to do at completion. In this case do nothing.
        /// </summary>
        public void OnCompleted()
        {
        }
    }
}
