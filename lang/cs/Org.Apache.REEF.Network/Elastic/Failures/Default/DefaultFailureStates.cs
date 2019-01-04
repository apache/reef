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

namespace Org.Apache.REEF.Network.Elastic.Failures.Default
{
    /// <summary>
    /// The default failure states.
    /// </summary>
    [Unstable("0.16", "The default states may change")]
    public enum DefaultFailureStates : int
    {
        Continue = 0, // When a failre is detected, just ignore it continue the computation

        ContinueAndReconfigure = 1, // When a failre is detected, continue the computation by properly reconfiguring the operators

        ContinueAndReschedule = 2, // When a failre is detected, continue the computation by reconfiguring the operators and try to reschedule the task

        StopAndReschedule = 3, // When a failre is detected, stop the computation and try to reschedule the task

        Fail = 4, // Fail

        Complete = 5 // Complete, final state


    }
}