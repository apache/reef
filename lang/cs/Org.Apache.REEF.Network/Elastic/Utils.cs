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

using Org.Apache.REEF.Utilities.Attributes;
using System.Globalization;

namespace Org.Apache.REEF.Network.Elastic
{
    /// <summary>
    /// Utility class.
    /// </summary>
    [Unstable("0.16", "Class may change or be removed")]
    internal static class Utils
    {
        /// <summary>
        /// Builds a task identifier out of a subscription(s) and an id.
        /// </summary>
        /// <param name="subscriptionName">The subscriptions active in the task</param>
        /// <param name="id">The task id</param>
        /// <returns>The task identifier</returns>
        public static string BuildTaskId(string subscriptionName, int id)
        {
            return BuildIdentifier("Task", subscriptionName, id);
        }

        /// <summary>
        /// Utility method returning an identifier by merging the input fields
        /// </summary>
        /// <param name="first">The first field</param>
        /// <param name="second">The second field</param>
        /// <param name="third">The third field</param>
        /// <returns>An id merging the three fields</returns>
        private static string BuildIdentifier(string first, string second, int third)
        {
            return string.Format(CultureInfo.InvariantCulture, "{0}-{1}-{2}", first, second, third);
        }
    }
}