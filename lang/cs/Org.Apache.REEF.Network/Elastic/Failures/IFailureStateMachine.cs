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

using Org.Apache.REEF.Network.Elastic.Failures.Default;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Utilities.Attributes;
using System;

namespace Org.Apache.REEF.Network.Elastic.Failures
{
    /// <summary>
    /// Where the decision is made on what to do when a failure happen.
    /// A decision is made based on the ratio between the initial data points
    /// and how many data points are lost.
    /// Decisions are in form of failure states and threshold levels.
    /// Failure machines should work as ladders, when some data is lost and the number
    /// of available data points move below / above one of the threshold, the state of the
    /// machine changes.
    /// </summary>
    [Unstable("0.16", "API may change")]
    [DefaultImplementation(typeof(DefaultFailureStateMachine))]
    public interface IFailureStateMachine
    {
        /// <summary>
        /// The machine current failure state.
        /// </summary>
        IFailureState State { get; }

        /// <summary>
        /// The total number of data points the machine was initialized with.
        /// </summary>
        int NumOfDataPoints { get; }

        /// <summary>
        /// The current number of data points data not reachable because of failures.
        /// </summary>
        int NumOfFailedDataPoints { get; }

        /// <summary>
        /// Method used to set or update the current threshold connected with
        /// a target failure state. The assumption is that higher failure states
        /// have higher thresholds.
        /// </summary>
        /// <param name="level">The failure state we want to change</param>
        /// <param name="threshold">A [0, 1] value specifying when the failure level is reached</param>
        void SetThreashold(IFailureState level, float threshold);

        /// <summary>
        /// A utility method for setting multiple threshold at once.
        /// </summary>
        /// <param name="weights">Pairs of failure states with related new thresholds</param>
        void SetThreasholds(Tuple<IFailureState, float>[] weights);

        /// <summary>
        /// Add new data point(s) to the failure machine.
        /// This method can be called either at initialization, or when
        /// new data points becomes available at runtime e.g., after a failure
        /// is resolved.
        /// </summary>
        /// <param name="points">How many data point to add</param>
        /// <param name="isNew">Whether the data point is new or restored from a previous failed points</param>
        /// <returns>The failure state resulting from the addition of the data points</returns>
        IFailureState AddDataPoints(int points, bool isNew);

        /// <summary>
        /// Remove data point(s) from the failure machine as a result of a runtime failure.
        /// </summary>
        /// <param name="points">How many data point to remove</param>
        /// <returns>A failure event resulting from the removal of the data points</returns>
        IFailureState RemoveDataPoints(int points);

        /// <summary>
        /// Signal the state machine to move into complete state.
        /// </summary>
        IFailureState Complete();

        /// <summary>
        /// Utility method used to clone the target failure machine.
        /// Only the thresholds are cloned, while the machine state is not.
        /// </summary>
        /// <param name="initalPoints">How many data points are avaialble in the new state machine</param>
        /// <param name="initalState">The state from which the new machine should start</param>
        /// <returns>A new failure machine with the same settings</returns>
        IFailureStateMachine Clone(int initalPoints = 0, int initalState = 0);
    }
}
