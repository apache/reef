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

using Org.Apache.REEF.Common.Context;
using Org.Apache.REEF.Common.Tasks;
using Org.Apache.REEF.Tang.Exceptions;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Tang.Util;
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.Network.Utilities
{
    internal class Utils
    {
        private static readonly Logger Log = Logger.GetLogger(typeof(Utils));

        /// <summary>
        /// Returns the TaskIdentifier from the Configuration.
        /// </summary>
        /// <param name="taskConfiguration">The Configuration object</param>
        /// <returns>The TaskIdentifier for the given Configuration</returns>
        public static string GetTaskId(IConfiguration taskConfiguration)
        {
            try
            {
                IInjector injector = TangFactory.GetTang().NewInjector(taskConfiguration);
                return injector.GetNamedInstance<TaskConfigurationOptions.Identifier, string>(
                    GenericType<TaskConfigurationOptions.Identifier>.Class);
            }
            catch (InjectionException)
            {
                Log.Log(Level.Error, "Unable to find task identifier");
                throw;
            }
        }

        /// <summary>
        /// Returns the Context Identifier from the Configuration.
        /// </summary>
        /// <param name="contextConfiguration">The Configuration object</param>
        /// <returns>The TaskIdentifier for the given Configuration</returns>
        public static string GetContextId(IConfiguration contextConfiguration)
        {
            try
            {
                IInjector injector = TangFactory.GetTang().NewInjector(contextConfiguration);
                return injector.GetNamedInstance<ContextConfigurationOptions.ContextIdentifier, string>(
                    GenericType<ContextConfigurationOptions.ContextIdentifier>.Class);
            }
            catch (InjectionException)
            {
                Log.Log(Level.Error, "Unable to find task identifier");
                throw;
            }
        }
    }
}
