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
using Org.Apache.REEF.Tang.Exceptions;
using Org.Apache.REEF.Utilities.Attributes;
using System;
using System.Collections.Generic;
using System.Linq;

/// <summary>
/// The default implementation of the failure state machine.
/// This implementation has 4 states:
/// - Continue the computation and ignore the failures
/// - Continue and reconfigure the operators based on the received failures
/// - Continue, reconfigure the operators while trying to reshedule failed tasks
/// - Stop the computation and try to reschedule the tasks
/// - Fail.
/// </summary>
namespace Org.Apache.REEF.Network.Elastic.Failures.Default
{
    [Unstable("0.16", "API may change")]
    public sealed class DefaultFailureStateMachine : IFailureStateMachine
    {
        private readonly object _statusLock = new object();

        private readonly SortedDictionary<DefaultFailureStates, DefaultFailureStates> transitionMapUp = 
            new SortedDictionary<DefaultFailureStates, DefaultFailureStates>()
        {
            { DefaultFailureStates.Continue, DefaultFailureStates.ContinueAndReconfigure },
            { DefaultFailureStates.ContinueAndReconfigure, DefaultFailureStates.ContinueAndReschedule },
            { DefaultFailureStates.ContinueAndReschedule, DefaultFailureStates.StopAndReschedule },
            { DefaultFailureStates.StopAndReschedule, DefaultFailureStates.Fail }
        };

        private readonly SortedDictionary<DefaultFailureStates, DefaultFailureStates> transitionMapDown = 
            new SortedDictionary<DefaultFailureStates, DefaultFailureStates>()
        {
            { DefaultFailureStates.ContinueAndReconfigure, DefaultFailureStates.Continue },
            { DefaultFailureStates.ContinueAndReschedule, DefaultFailureStates.ContinueAndReconfigure },
            { DefaultFailureStates.StopAndReschedule, DefaultFailureStates.ContinueAndReschedule },
            { DefaultFailureStates.Fail, DefaultFailureStates.StopAndReschedule }
        };

        private readonly IDictionary<DefaultFailureStates, float> transitionWeights = 
            new Dictionary<DefaultFailureStates, float>()
        {
            { DefaultFailureStates.ContinueAndReconfigure, 0.01F },
            { DefaultFailureStates.ContinueAndReschedule, 0.40F },
            { DefaultFailureStates.StopAndReschedule, 0.60F },
            { DefaultFailureStates.Fail, 0.80F }
        };

        private static List<int> canMoveToComplete = new List<int>()
        {
            (int)DefaultFailureStates.Continue,
            (int)DefaultFailureStates.ContinueAndReconfigure,
            (int)DefaultFailureStates.ContinueAndReschedule,
            (int)DefaultFailureStates.Complete
        };

        private static List<int> isFinalState = new List<int>()
        {
            (int)DefaultFailureStates.Complete
        };

        /// <summary>
        /// Default failure state machine starting with 0 data points and in continue state.
        /// </summary>
        [Inject]
        public DefaultFailureStateMachine() : this(0, DefaultFailureStates.Continue)
        {
        }

        /// <summary>
        /// Default failure stata machine starting with a given amount of data points and a given
        /// intial state.
        /// </summary>
        /// <param name="initalPoints">The number of initial data points for the machine, 0 by default</param>
        /// <param name="initalState">The initial state, continue by default</param>
        public DefaultFailureStateMachine(
            int initalPoints = 0, 
            DefaultFailureStates initalState = DefaultFailureStates.Continue)
        {
            NumOfDataPoints = initalPoints;
            NumOfFailedDataPoints = initalPoints;
            State = new DefaultFailureState((int)initalState);
        }

        /// <summary>
        /// The machine current failure state.
        /// </summary>
        public IFailureState State { get; private set; }

        /// <summary>
        /// The total number of data points the machine was initialized with.
        /// </summary>
        public int NumOfDataPoints { get; private set; }

        /// <summary>
        /// The current number of data points data not reachable because of failures.
        /// </summary>>
        public int NumOfFailedDataPoints { get; private set; }

        /// <summary>
        /// Add new data point(s) to the failure machine.
        /// This method can be called either at initialization, or when
        /// new data points becomes available at runtime e.g., after a failure
        /// is resolved.
        /// </summary>
        /// <param name="points">How many data point to add</param>
        /// <param name="isNew">Whether the data point is new or restored from a previous failed points</param>
        /// <returns>The failure state resulting from the addition of the data points</returns>
        public IFailureState AddDataPoints(int points, bool isNew)
        {
            lock (_statusLock)
            {
                if (isFinalState.Contains(State.FailureState))
                {
                    return State;
                }

                if (isNew)
                {
                    NumOfDataPoints += points;
                }
                else
                {
                    NumOfFailedDataPoints -= points;
                }
                if (State.FailureState > (int)DefaultFailureStates.Continue && 
                    State.FailureState <= (int)DefaultFailureStates.Fail)
                {
                    float currentRate = (float)NumOfFailedDataPoints / NumOfDataPoints;

                    while (State.FailureState > (int)DefaultFailureStates.Continue && 
                        currentRate < transitionWeights[(DefaultFailureStates)State.FailureState])
                    {
                        State.FailureState = (int)transitionMapDown[(DefaultFailureStates)State.FailureState];
                    }
                }

                return State;
            }
        }

        /// <summary>
        /// Remove data point(s) from the failure machine as a result of a runtime failure.
        /// </summary>
        /// <param name="points">How many data point to remove</param>
        /// <returns>A failure event resulting from the removal of the data points</returns>
        public IFailureState RemoveDataPoints(int points)
        {
            lock (_statusLock)
            {
                NumOfFailedDataPoints += points;

                float currentRate = (float)NumOfFailedDataPoints / NumOfDataPoints;

                if (isFinalState.Contains(State.FailureState) && 
                    currentRate >= transitionWeights[DefaultFailureStates.StopAndReschedule])
                {
                    throw new IllegalStateException("Received remove data point when state is complete: failing.");
                }

                while (State.FailureState < (int)DefaultFailureStates.Fail &&
                    currentRate > transitionWeights[transitionMapUp[(DefaultFailureStates)State.FailureState]])
                {
                    State.FailureState = (int)transitionMapUp[(DefaultFailureStates)State.FailureState];
                }

                return State;
            }
        }

        /// <summary>
        /// Signal the state machine to move into complete state.
        /// </summary>
        public IFailureState Complete()
        {
            lock (_statusLock)
            {
                if (canMoveToComplete.Contains(State.FailureState))
                {
                    State.FailureState = (int)DefaultFailureStates.Complete;
                }
                else
                {
                    throw new IllegalStateException(
                        $"Failure machine cannot move from state {State.FailureState} to Complete: failing.");
                }
            }

            return State;
        }

        /// <summary>
        /// Method used to set or update the current threshold connected with
        /// a target failure state. The assumption is that higher failure states
        /// have higher thresholds.
        /// </summary>
        /// <param name="level">The failure state we want to change</param>
        /// <param name="threshold">A [0, 1] value specifying when the failure level is reached</param>
        public void SetThreshold(IFailureState level, float threshold)
        {
            if (!(level is DefaultFailureState))
            {
                throw new ArgumentException(level.GetType() + " is not DefaultFailureStateMachine");
            }

            if (level.FailureState == (int)DefaultFailureStates.Continue)
            {
                throw new ArgumentException("Cannot change the threshold for Continue state");
            }

            lock (_statusLock)
            {
                transitionWeights[(DefaultFailureStates)level.FailureState] = threshold;

                CheckConsistency();
            }
        }

        /// <summary>
        /// A utility method for setting multiple threshold at once.
        /// </summary>
        /// <param name="weights">Pairs of failure states with related new thresholds</param>
        public void SetThresholds(params Tuple<IFailureState, float>[] weights)
        {
            if (!weights.All(weight => weight.Item1 is DefaultFailureState))
            {
                throw new ArgumentException("Input is not of type DefaultFailureStateMachine,");
            }

            if (weights.Any(weight => weight.Item1.FailureState == (int)DefaultFailureStates.Continue))
            {
                throw new ArgumentException("Cannot change the threshold for Continue state.");
            }

            lock (_statusLock)
            {
                foreach (Tuple<IFailureState, float> weight in weights)
                {
                    transitionWeights[(DefaultFailureStates)weight.Item1.FailureState] = weight.Item2;
                }

                CheckConsistency();
            }
        }

        /// <summary>
        /// Utility method used to clone the target failure machine.
        /// Only the thresholds are cloned, while the machine state is not.
        /// </summary>
        /// <param name="initalPoints">How many data points are avaialble in the new state machine</param>
        /// <param name="initalState">The state from which the new machine should start</param>
        /// <returns>A new failure machine with the same settings</returns>
        public IFailureStateMachine Clone(
            int initalPoints = 0, 
            int initalState = (int)DefaultFailureStates.Continue)
        {
            var newMachine = new DefaultFailureStateMachine(initalPoints, (DefaultFailureStates)initalState);

            foreach (DefaultFailureStates state in transitionWeights.Keys.OrderByDescending(x => x))
            {
                newMachine.SetThreshold(new DefaultFailureState((int)state), transitionWeights[state]);
            }

            return newMachine;
        }

        /// <summary>
        /// Check if the states and related thresholds and consistent: i.e., each state can move 
        /// up or down to only one other state.
        /// </summary>
        private void CheckConsistency()
        {
            lock (_statusLock)
            {
                var state = DefaultFailureStates.ContinueAndReconfigure;
                float prevWeight = transitionWeights[state];
                state = transitionMapUp[state];
                float nextWeight = transitionWeights[state];

                while (nextWeight >= 0)
                {
                    if (nextWeight < prevWeight)
                    {
                        throw new IllegalStateException(
                            $"State {transitionMapDown[state]} weight is bigger than state {state}.");
                    }

                    prevWeight = nextWeight;

                    if (state == DefaultFailureStates.StopAndReschedule)
                    {
                        return;
                    }

                    state = transitionMapUp[state];
                    transitionWeights.TryGetValue(state, out nextWeight);
                }
            }
        }
    }
}
