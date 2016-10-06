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

using Org.Apache.REEF.Tang.Annotations;

namespace Org.Apache.REEF.Common.Metrics.MetricsSystem.Parameters
{
    /// <summary>
    /// Source parameters in metrics system start context.
    /// </summary>
    internal sealed class ContextSourceParameters
    {
        [NamedParameter("Name of the source in metrics system context", defaultValue: "default-source")]
        public sealed class SourceName : Name<string>
        {
        }

        [NamedParameter("Description of the source in metrics system context", defaultValue: "default-desc")]
        public sealed class SourceDesc : Name<string>
        {
        }
    }
}
