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

using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Globalization;
using System.Linq;
using Org.Apache.REEF.Driver.Context;
using Org.Apache.REEF.Driver.Evaluator;
using Org.Apache.REEF.Utilities.Attributes;
using Org.Apache.REEF.Utilities.Diagnostics;
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.IMRU.OnREEF.Driver
{
    /// <summary>
    /// Manages active contexts for the driver
    /// </summary>
    [NotThreadSafe]
    internal sealed class ActiveContextManager : IDisposable
    {
        private static readonly Logger Logger = Logger.GetLogger(typeof(ActiveContextManager));
        private readonly IDictionary<string, IActiveContext> _activeContexts = new Dictionary<string, IActiveContext>();
        private readonly int _totalExpectedContexts;
        private IObserver<int> _activeContextObserver;

        /// <summary>
        /// Constructor of ActiveContextManager
        /// totalContexts specify the total number of expected active contexts that driver needs
        /// activeContextObserver will be notified when all active contexts are received. 
        /// </summary>
        /// <param name="totalContexts"></param>
        internal ActiveContextManager(int totalContexts)
        {
            _totalExpectedContexts = totalContexts;
        }

        /// <summary>
        /// Returns the collection of IActiveContext
        /// </summary>
        internal IReadOnlyCollection<IActiveContext> ActiveContexts
        {
            get { return new ReadOnlyCollection<IActiveContext>(_activeContexts.Values.ToList()); }
        }

        /// <summary>
        /// Returns the difference between the number of expected IActiveContext and actually number of IActiveContext. 
        /// </summary>
        internal int NumberOfMissingContexts
        {
            get { return _totalExpectedContexts - NumberOfActiveContexts; }
        }

        /// <summary>
        /// Subscribe an observer of ActiveContextManager
        /// </summary>
        /// <param name="activeContextObserver"></param>
        /// <returns></returns>
        public IDisposable Subscribe(IObserver<int> activeContextObserver)
        {
            if (_activeContextObserver != null)
            {
                return null;
            }
            _activeContextObserver = activeContextObserver;
            return this;
        }

        /// <summary>
        /// Checks if all the requested contexts are received. 
        /// </summary>
        internal bool AreAllContextsReceived
        {
            get { return _totalExpectedContexts == NumberOfActiveContexts; }
        }

        /// <summary>
        /// Adds an IActiveContext to the ActiveContext collection
        /// Throws IMRUSystemException if the IActiveContext already exists or NumberOfActiveContexts has exceeded the total expected contexts
        /// </summary>
        /// <param name="activeContext"></param>
        internal void Add(IActiveContext activeContext)
        {
            if (_activeContexts.ContainsKey(activeContext.Id))
            {
                var msg = string.Format(CultureInfo.InvariantCulture, "The context [{0}] received already exists.", activeContext.Id);
                Exceptions.Throw(new IMRUSystemException(msg), Logger);
            }

            if (NumberOfActiveContexts >= _totalExpectedContexts)
            {
                var msg = string.Format(CultureInfo.InvariantCulture, "Trying to add an extra active context {0}. The total number of the active contexts has reached to the expected number {1}.", activeContext.Id, _totalExpectedContexts);
                Exceptions.Throw(new IMRUSystemException(msg), Logger);
            }

            _activeContexts.Add(activeContext.Id, activeContext);

            if (AreAllContextsReceived && _activeContextObserver != null)
            {
                _activeContextObserver.OnNext(_activeContexts.Count);
            }
        }

        /// <summary>
        /// Removes an IActiveContext from the ActiveContext collection
        /// Throws IMRUSystemException if the IActiveContext doesn't exist.
        /// </summary>
        /// <param name="activeContextId"></param>
        internal void Remove(string activeContextId)
        {
            if (!_activeContexts.ContainsKey(activeContextId))
            {
                var msg = string.Format(CultureInfo.InvariantCulture, "The context [{0}] to be removed does not exist.", activeContextId);
                Exceptions.Throw(new IMRUSystemException(msg), Logger);
            }
            _activeContexts.Remove(activeContextId);
        }

        /// <summary>
        /// Returns the current number of IActiveContext in the ActiveContext collection
        /// </summary>
        internal int NumberOfActiveContexts
        {
            get { return _activeContexts.Count; }
        }

        /// <summary>
        /// Given an IFailedEvaluator, removes associated IActiveContext from the collection
        /// Throws IMRUSystemException if associated IActiveContext doesn't exist or
        /// if more than one IActiveContexts are associated with the IFailedEvaluator
        /// as current IMRU driver assumes that there is only one context associated with the IFailedEvalutor
        /// </summary>
        /// <param name="value"></param>
        internal void RemoveFailedContextInFailedEvaluator(IFailedEvaluator value)
        {
            if (value.FailedContexts != null && value.FailedContexts.Count > 0)
            {
                if (value.FailedContexts.Count == 1)
                {
                    var failedContextId = value.FailedContexts[0].Id;
                    if (!_activeContexts.Remove(failedContextId))
                    {
                        var msg = string.Format(CultureInfo.InvariantCulture,
                            "The active context [{0}] attached in IFailedEvaluator [{1}] is not in the Active Contexts collection.",
                            failedContextId,
                            value.Id);
                        Exceptions.Throw(new IMRUSystemException(msg), Logger);
                    }
                }
                else
                {
                    var msg = string.Format(CultureInfo.InvariantCulture,
                        "There are [{0}] contexts attached in the failed evaluator. Expected number is 1.",
                        value.FailedContexts.Count);
                    Exceptions.Throw(new IMRUSystemException(msg), Logger);
                }
            }
        }

        /// <summary>
        /// sets _activeContextObserver to null 
        /// </summary>
        public void Dispose()
        {
            _activeContextObserver = null;
        }
    }
}