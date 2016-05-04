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

namespace Org.Apache.REEF.IMRU.OnREEF.Driver
{
    /// <summary>
    /// Specification for an Evaluator
    /// </summary>
    internal sealed class EvaluatorSpecification
    {
        /// <summary>
        /// Create an EvaluatorSpecification
        /// </summary>
        /// <param name="megabytes"></param>
        /// <param name="core"></param>
        internal EvaluatorSpecification(int megabytes, int core)
        {
            Megabytes = megabytes;
            Core = core;
        }

        /// <summary>
        /// Size of the memory for the Evaluator request
        /// </summary>
        internal int Megabytes { get; private set; }

        /// <summary>
        /// Number of core for the Evaluator request
        /// </summary>
        internal int Core { get; private set; }
    }
}
