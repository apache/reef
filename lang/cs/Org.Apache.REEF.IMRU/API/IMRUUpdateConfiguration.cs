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

using Org.Apache.REEF.Tang.Formats;
using Org.Apache.REEF.Tang.Util;

namespace Org.Apache.REEF.IMRU.API
{
    /// <summary>
    /// A configuration module for IMRU Update function.
    /// </summary>
    /// <typeparam name="TMapInput">The type of the side information provided to the Map function</typeparam>
    /// <typeparam name="TMapOutput">The return type of the Map function</typeparam>
    /// <typeparam name="TResult">The return type of the computation.</typeparam>
    public sealed class IMRUUpdateConfiguration<TMapInput, TMapOutput, TResult> : ConfigurationModuleBuilder
    {
        /// <summary>
        /// The IUpdateFunction type to use.
        /// </summary>
        public static readonly RequiredImpl<IUpdateFunction<TMapInput, TMapOutput, TResult>> UpdateFunction =
            new RequiredImpl<IUpdateFunction<TMapInput, TMapOutput, TResult>>();

        /// <summary>
        /// The task progress reporter to use.
        /// </summary>
        public static readonly OptionalImpl<ITaskProgressReporter> TaskProgressReporter =
            new OptionalImpl<ITaskProgressReporter>();

        /// <summary>
        /// Configuration module
        /// </summary>
        public static ConfigurationModule ConfigurationModule =
            new IMRUUpdateConfiguration<TMapInput, TMapOutput, TResult>()
                .BindImplementation(GenericType<IUpdateFunction<TMapInput, TMapOutput, TResult>>.Class, UpdateFunction)
                .BindImplementation(GenericType<ITaskProgressReporter>.Class, TaskProgressReporter)
                .Build();
    }
}