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
using System;

namespace Org.Apache.REEF.Network.Elastic.Operators
{
    /// <summary>
    /// Constants labeling the set of available operators.
    /// </summary>
    [Unstable("0.16", "Constants may change")]
    public enum OperatorType : int
    {
        Empty = 0,
        Broadcast = 1,
        Reduce = 2,
        AggregationRing = 3,
        Iterate = 4,
        Scatter = 5,
        Gather = 6
    }

    public static class OperatorTypeToString
    {
        public static string ToString(this OperatorType type)
        {
            switch (type)
            {
                case OperatorType.Empty: return "Empty";
                case OperatorType.Broadcast: return "Broadcast";
                case OperatorType.Reduce: return "Reduce";
                case OperatorType.AggregationRing: return "AggregationRing";
                case OperatorType.Iterate: return "Iterate";
                case OperatorType.Scatter: return "Scatter";
                case OperatorType.Gather: return "Gather";
                default: throw new ArgumentException($"Operator type {type} not found");
            }
        }
    }
}