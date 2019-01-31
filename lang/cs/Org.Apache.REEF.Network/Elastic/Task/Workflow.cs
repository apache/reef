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

using Org.Apache.REEF.Network.Elastic.Failures;
using Org.Apache.REEF.Network.Elastic.Operators;
using Org.Apache.REEF.Network.Elastic.Operators.Physical;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Utilities.Attributes;
using Org.Apache.REEF.Utilities.Logging;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using static Org.Apache.REEF.Network.Elastic.Config.GroupCommunicationConfigurationOptions;

namespace Org.Apache.REEF.Network.Elastic.Task
{
    /// <summary>
    /// Task-side representation of the the sequence of group communication operations to execute.
    /// Exception rised during execution are managed by the framework and recovered through
    /// the user-defined policies / mechanisms.
    /// </summary>
    [Unstable("0.16", "API may change")]
    public sealed class Workflow : IEnumerator<IElasticOperator>, IEnumerable<IElasticOperator>
    {
        private static readonly Logger Log = Logger.GetLogger(typeof(Workflow));

        private int _position = -1;
        private bool _failed = false;
        private bool _disposed = false;
        private readonly List<int> _iteratorsPosition = new List<int>();

        private readonly object _lock = new object();
        private readonly IList<IElasticOperator> _operators = new List<IElasticOperator>();
        private readonly CancellationSource _cancellationSource;
        private readonly bool _isRescheduled;

        /// <summary>
        /// Injectable constructor.
        /// </summary>
        /// <param name="cancellationSource"></param>
        [Inject]
        private Workflow(
            [Parameter(typeof(IsRescheduled))] bool isRescheduled,
            CancellationSource cancellationSource)
        {
            _cancellationSource = cancellationSource;
            _isRescheduled = isRescheduled;
        }

        /// <summary>
        /// Try to move to the next operation in the workflow.
        /// </summary>
        /// <returns></returns>
        public bool MoveNext()
        {
            _position++;

            if (_failed || _cancellationSource.IsCancelled)
            {
                return false;
            }

            // Check if we need to iterate
            if (_iteratorsPosition.Count > 0 && _position == _iteratorsPosition[0])
            {
                var iteratorOperator = _operators[_position] as IElasticIterator;

                if (iteratorOperator.MoveNext())
                {
                    _position++;
                    ResetOperatorPositions();

                    return true;
                }
                else
                {
                    if (_iteratorsPosition.Count > 1)
                    {
                        _iteratorsPosition.RemoveAt(0);
                        _position = _iteratorsPosition[0] - 1;
                    }

                    return false;
                }
            }

            // In case we have one or zero iterators
            // (or we are at the last iterator when multiple iterators exists)
            if (_position >= _operators.Count ||
                (_iteratorsPosition.Count > 1 && _position == _iteratorsPosition[1]))
            {
                if (_iteratorsPosition.Count == 0)
                {
                    return false;
                }
                else
                {
                    _position = _iteratorsPosition[0] - 1;

                    return MoveNext();
                }
            }

            if (_isRescheduled)
            {
                Current.OnTaskRescheduled.Invoke();
            }

            return true;
        }

        /// <summary>
        /// Method used to make the framework aware that an exception as been thrown
        /// during execution.
        /// </summary>
        /// <param name="e">The rised exception</param>
        public void Throw(Exception e)
        {
            if (_cancellationSource.IsCancelled)
            {
                Log.Log(Level.Warning,
                    "Workflow captured an exception while cancellation source was true.", e);
            }
            else
            {
                Log.Log(Level.Error, "Workflow captured an exception.", e);
                _failed = true;

                throw new OperatorException(
                    "Workflow captured an exception", Current.OperatorId, e, Current.FailureInfo);
            }
        }

        /// <summary>
        /// Start the execution of the workflow from the first operator / iterator.
        /// </summary>
        public void Reset()
        {
            if (_iteratorsPosition.Count > 0)
            {
                _position = _iteratorsPosition[0];
            }
            else
            {
                _position = 0;
            }
        }

        /// <summary>
        /// Get the current elastic operator.
        /// </summary>
        public IElasticOperator Current
        {
            get
            {
                return _position == -1 ? _operators[0] : _operators[_position];
            }
        }

        object IEnumerator.Current
        {
            get { return Current; }
        }

        /// <summary>
        /// Dispose the workflow.
        /// </summary>
        public void Dispose()
        {
            lock (_lock)
            {
                if (!_disposed)
                {
                    if (_operators != null)
                    {
                        // Clean dispose, check that the computation is completed
                        if (!_failed)
                        {
                            foreach (var op in _operators)
                            {
                                op?.WaitCompletionBeforeDisposing();
                            }
                        }
                    }

                    foreach (var op in _operators)
                    {
                        op?.Dispose();
                    }
                }

                _disposed = true;
            }
        }

        /// <summary>
        /// Add an elastic operator to the workflow.
        /// </summary>
        /// <param name="op"></param>
        internal void Add(IElasticOperator op)
        {
            op.CancellationSource = _cancellationSource.Source;

            _operators.Add(op);

            if (_iteratorsPosition.Count > 0)
            {
                var iterPos = _iteratorsPosition.Last();
                var iterator = _operators[iterPos] as IElasticIterator;

                op.IteratorReference = iterator;
                iterator.RegisterActionOnTaskRescheduled(op.OnTaskRescheduled);
            }

            if (op.OperatorType == OperatorType.Iterate)
            {
                _iteratorsPosition.Add(_operators.Count - 1);
            }
        }

        /// <summary>
        /// Initializes the communication group.
        /// Computation blocks until all required tasks are registered in the group.
        /// </summary>
        /// <param name="cancellationSource">The signal to cancel the operation</param>
        internal void WaitForTaskRegistration(CancellationTokenSource cancellationSource = null)
        {
            foreach (var op in _operators)
            {
                op.WaitForTaskRegistration(cancellationSource);
            }
        }

        /// <summary>
        /// Reset the position tracker for all operators in the workflow.
        /// </summary>
        private void ResetOperatorPositions()
        {
            for (int pos = _position; pos < _operators.Count; pos++)
            {
                _operators[pos].ResetPosition();
            }
        }

        public IEnumerator<IElasticOperator> GetEnumerator()
        {
            return this;
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return this;
        }
    }
}