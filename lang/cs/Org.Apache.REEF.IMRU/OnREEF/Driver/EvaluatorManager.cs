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
// software distributed under the License is distributed on 
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

using System.Collections.Generic;
using Org.Apache.REEF.Driver.Evaluator;
using Org.Apache.REEF.Utilities.Attributes;
using Org.Apache.REEF.Utilities.Diagnostics;
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.IMRU.OnREEF.Driver
{
    /// <summary>
    /// Manages allocated and failed Evaluators for driver.
    /// It keeps tracking allocated Evaluators and failed Evaluator ids. Provides methods to 
    /// add, remove evaluators from the collections so that to provide data for fault tolerant.
    /// It also tracks master evaluator. 
    /// </summary>
    [NotThreadSafe]
    internal sealed class EvaluatorManager
    {
        private static readonly Logger Logger = Logger.GetLogger(typeof(EvaluatorManager));
        internal const string MasterBatchId = "MasterBatchId";
        internal const string MapperBatchId = "MapperBatchId";

        private readonly ISet<string> _allocatedEvaluators = new HashSet<string>();
        private readonly ISet<string> _failedEvaluators = new HashSet<string>();

        private readonly int _totalExpectedEvaluators;
        private readonly int _allowedNumberOfEvaluatorFailures;
        private readonly IEvaluatorRequestor _evaluatorRequestor;
        private string _masterEvaluatorId;

        private readonly EvaluatorSpecification _updateEvaluatorSpecification;
        private readonly EvaluatorSpecification _mapperEvaluatorSpecification;

        /// <summary>
        /// Creates a EvaluatorManager for driver which contains specification for the Evaluators
        /// </summary>
        /// <param name="totalEvaluators"></param>
        /// <param name="allowedNumberOfEvaluatorFailures"></param>
        /// <param name="evaluatorRequestor"></param>
        /// <param name="updateEvaluatorSpecification"></param>
        /// <param name="mapperEvaluatorSpecification"></param>
        internal EvaluatorManager(
            int totalEvaluators,
            int allowedNumberOfEvaluatorFailures,
            IEvaluatorRequestor evaluatorRequestor,
            EvaluatorSpecification updateEvaluatorSpecification,
            EvaluatorSpecification mapperEvaluatorSpecification)
        {
            _totalExpectedEvaluators = totalEvaluators;
            _allowedNumberOfEvaluatorFailures = allowedNumberOfEvaluatorFailures;
            _evaluatorRequestor = evaluatorRequestor;
            _updateEvaluatorSpecification = updateEvaluatorSpecification;
            _mapperEvaluatorSpecification = mapperEvaluatorSpecification;
        }

        /// <summary>
        /// Request update/master Evaluator from resource manager
        /// </summary>
        internal void RequestUpdateEvaluator()
        {
            _evaluatorRequestor.Submit(
                _evaluatorRequestor.NewBuilder()
                    .SetCores(_updateEvaluatorSpecification.Core)
                    .SetMegabytes(_updateEvaluatorSpecification.Megabytes)
                    .SetNumber(1)
                    .SetEvaluatorBatchId(MasterBatchId)
                    .Build());
        }

        /// <summary>
        /// Request map evaluators from resource manager
        /// </summary>
        /// <param name="numEvaluators">Number of evaluators to request</param>
        internal void RequestMapEvaluators(int numEvaluators)
        {
            _evaluatorRequestor.Submit(
                _evaluatorRequestor.NewBuilder()
                    .SetMegabytes(_mapperEvaluatorSpecification.Megabytes)
                    .SetNumber(numEvaluators)
                    .SetCores(_mapperEvaluatorSpecification.Core)
                    .SetEvaluatorBatchId(MapperBatchId)
                    .Build());
        }

        /// <summary>
        /// Add an Evaluator id to _allocatedEvaluators.
        /// If the IAllocatedEvaluator is for master, set master Evaluator id
        /// IMRUSystemException will be thrown in the following cases:
        ///   The Evaluator Id is already in the allocated Evaluator collection
        ///   The added IAllocatedEvaluator is the last one expected, and master Evaluator is still not added yet
        ///   The number of AllocatedEvaluators has reached the total expected Evaluators
        /// </summary>
        /// <param name="evaluator"></param>
        internal void AddAllocatedEvaluator(IAllocatedEvaluator evaluator)
        {
            if (IsAllocatedEvaluator(evaluator.Id))
            {
                string msg = string.Format("The allocated evaluator {0} already exists.", evaluator.Id);
                Exceptions.Throw(new IMRUSystemException(msg), Logger);
            }

            if (IsEvaluatorForMaster(evaluator))
            {
                SetMasterEvaluatorId(evaluator.Id);
            }

            if (_masterEvaluatorId == null && NumberOfAllocatedEvaluators + 1 == _totalExpectedEvaluators)
            {
                string msg = string.Format("Trying to add the last Evaluator {0} but master Evaluator is not added yet.", evaluator.Id);
                Exceptions.Throw(new IMRUSystemException(msg), Logger);
            }

            if (NumberOfAllocatedEvaluators == _totalExpectedEvaluators)
            {
                string msg = string.Format("Trying to add an additional Evaluator {0}, but the total expected Evaluator number {1} has been reached.", evaluator.Id, _totalExpectedEvaluators);
                Exceptions.Throw(new IMRUSystemException(msg), Logger);
            }
            _allocatedEvaluators.Add(evaluator.Id);
        }

        /// <summary>
        /// Remove an Evaluator from allocated Evaluator collection by evaluator id.
        /// If the given evaluator id is not in allocated Evaluator collection, throw IMRUSystemException.
        /// </summary>
        /// <param name="evaluatorId"></param>
        internal void RemoveAllocatedEvaluator(string evaluatorId)
        {
            if (!IsAllocatedEvaluator(evaluatorId))
            {
                string msg = string.Format("The allocated evaluator to be removed {0} does not exist.", evaluatorId);
                Exceptions.Throw(new IMRUSystemException(msg), Logger);
            }
            _allocatedEvaluators.Remove(evaluatorId);
        }

        /// <summary>
        /// Returns number of allocated Evaluators
        /// </summary>
        internal int NumberOfAllocatedEvaluators
        {
            get { return _allocatedEvaluators.Count; }
        }

        /// <summary>
        /// Checks if the Evaluator with the specified id has been allocated
        /// </summary>
        /// <param name="evaluatorId"></param>
        /// <returns></returns>
        internal bool IsAllocatedEvaluator(string evaluatorId)
        {
            return _allocatedEvaluators.Contains(evaluatorId);
        }

        /// <summary>
        /// Checks if all the expected Evaluators are allocated.
        /// </summary>
        internal bool AreAllEvaluatorsAllocated
        {
            get { return _totalExpectedEvaluators == NumberOfAllocatedEvaluators && _masterEvaluatorId != null; }
        }

        /// <summary>
        /// Records failed Evaluator
        /// Removes it from allocated Evaluator and adds it to the failed Evaluators collection
        /// If the evaluatorId is not in _failedEvaluators, throw IMRUSystemException
        /// </summary>
        /// <param name="evaluatorId"></param>
        internal void RecordFailedEvaluator(string evaluatorId)
        {
            RemoveAllocatedEvaluator(evaluatorId);

            if (_failedEvaluators.Contains(evaluatorId))
            {
                string msg = string.Format("The failed evaluator {0} has been recorded.", evaluatorId);
                Exceptions.Throw(new IMRUSystemException(msg), Logger);
            }
            _failedEvaluators.Add(evaluatorId);
        }

        /// <summary>
        /// Checks if the number of failed Evaluators has reached allowed maximum number of evaluator failures 
        /// </summary>
        internal bool ReachedMaximumNumberOfEvaluatorFailures
        {
            get { return _failedEvaluators.Count >= AllowedNumberOfEvaluatorFailures; }
        }

        /// <summary>
        /// Returns allowed maximum number of evaluator failures
        /// </summary>
        internal int AllowedNumberOfEvaluatorFailures
        {
            get { return _allowedNumberOfEvaluatorFailures; }
        }

        /// <summary>
        /// Reset failed Evaluator collection
        /// </summary>
        internal void ResetFailedEvaluators()
        {
            if (IsMasterEvaluatorFailed)
            {
                ResetMasterEvaluatorId();
            }
            _failedEvaluators.Clear();
        }

        /// <summary>
        /// Sets master Evaluator id.
        /// Throws IMRUSystemException if evaulatorId is null or _masterEvaluatorId is not null.
        /// </summary>
        /// <param name="evaluatorId"></param>
        private void SetMasterEvaluatorId(string evaluatorId)
        {
            if (evaluatorId == null)
            {
                Exceptions.Throw(new IMRUSystemException("Master evaluatorId should not be null."), Logger);
            }

            if (_masterEvaluatorId != null)
            {
                string msg = string.Format("There is already a master evaluator {0}", _masterEvaluatorId);
                Exceptions.Throw(new IMRUSystemException(msg), Logger);
            }
            _masterEvaluatorId = evaluatorId;
        }

        /// <summary>
        /// Sets master Evaluator id to null.
        /// If the master Evaluator is already null, throw IMRUSystemException
        /// </summary>
        private void ResetMasterEvaluatorId()
        {
            if (_masterEvaluatorId == null)
            {
                Exceptions.Throw(new IMRUSystemException("Master evaluator is already null"), Logger);
            }
            _masterEvaluatorId = null;
        }

        /// <summary>
        /// Checks if the IAllocatedEvaluator is for master
        /// </summary>
        /// <param name="evaluator"></param>
        /// <returns></returns>
        internal bool IsEvaluatorForMaster(IAllocatedEvaluator evaluator)
        {
            return evaluator.EvaluatorBatchId.Equals(MasterBatchId);
        }

        /// <summary>
        /// Checks if the evaluator id is the master evaluator id
        /// </summary>
        /// <param name="evaluatorId"></param>
        /// <returns></returns>
        internal bool IsMasterEvaluatorId(string evaluatorId)
        {
            if (_masterEvaluatorId != null)
            {
                return _masterEvaluatorId.Equals(evaluatorId);
            }
            return false;
        }

        /// <summary>
        /// Checks if the master Evaluator failed
        /// </summary>
        /// <returns></returns>
        internal bool IsMasterEvaluatorFailed
        {
            get
            {
                return _masterEvaluatorId != null && _failedEvaluators.Contains(_masterEvaluatorId);
            }
        }

        /// <summary>
        /// Returns number of failed mapper Evaluators
        /// </summary>
        /// <returns></returns>
        internal int NumberofFailedMappers
        {
            get
            {
                if (IsMasterEvaluatorFailed)
                {
                    return _failedEvaluators.Count - 1;
                }
                return _failedEvaluators.Count;
            }
        }

        /// <summary>
        /// Returns number of missing Evaluators
        /// </summary>
        internal int NumberOfMissingEvaluators
        {
            get { return _totalExpectedEvaluators - NumberOfAllocatedEvaluators; }
        }
    }
}