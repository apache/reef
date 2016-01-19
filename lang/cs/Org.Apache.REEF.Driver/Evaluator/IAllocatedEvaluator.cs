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
using Org.Apache.REEF.Common;
using Org.Apache.REEF.Common.Evaluator;
using Org.Apache.REEF.Utilities;

namespace Org.Apache.REEF.Driver.Evaluator
{
    /// <summary>
    /// Represents an Evaluator that is allocated, but is not running yet.
    /// </summary>
    public interface IAllocatedEvaluator : IDisposable, IIdentifiable, IContextSubmittable, IContextAndTaskSubmittable, ITaskSubmittable
    {
        EvaluatorType Type { get; set; }

        string NameServerInfo { get; set; }

        string EvaluatorBatchId { get; set; }

        IEvaluatorDescriptor GetEvaluatorDescriptor();

        /// <summary>
        /// Puts the given file into the working directory of the Evaluator.
        /// </summary>
        /// <param name="file">the file to be copied</param>
        void AddFile(string file);

        /// <summary>
        /// Puts the given file into the working directory of the Evaluator and adds it to its classpath.
        /// </summary>
        /// <param name="file">the file to be copied</param>
        void AddLibrary(string file);

        void AddFileResource(string file);
    }
}
