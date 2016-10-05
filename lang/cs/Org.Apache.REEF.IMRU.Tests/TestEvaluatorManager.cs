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
using NSubstitute;
using Org.Apache.REEF.Driver.Evaluator;
using Org.Apache.REEF.IMRU.OnREEF.Driver;
using Xunit;

namespace Org.Apache.REEF.IMRU.Tests
{
    /// <summary>
    /// Test cases for EvaluatorManager
    /// </summary>
    public sealed class TestEvaluatorManager
    {
        private const string EvaluatorIdPrefix = "EvaluatorId";
        private int _masterBatchIdSquenceNumber = 0;
        private int _mapperBatchIdSquenceNumber = 0;

        /// <summary>
        /// Test valid add, remove Evaluators
        /// </summary>
        [Fact]
        public void TestValidAddRemoveAllocatedEvaluator()
        {
            var evaluatorManager = CreateTestEvaluators(3, 1);
            Assert.Equal(3, evaluatorManager.NumberOfAllocatedEvaluators);
            Assert.True(evaluatorManager.AreAllEvaluatorsAllocated());
            Assert.True(evaluatorManager.IsMasterEvaluatorId(EvaluatorIdPrefix + 1));
            Assert.False(evaluatorManager.IsMasterEvaluatorId(EvaluatorIdPrefix + 2));
            Assert.True(evaluatorManager.IsAllocatedEvaluator(EvaluatorIdPrefix + 2));
            Assert.False(evaluatorManager.IsMasterEvaluatorFailed());

            evaluatorManager.RecordFailedEvaluator(EvaluatorIdPrefix + 1);
            Assert.Equal(2, evaluatorManager.NumberOfAllocatedEvaluators);
            Assert.True(evaluatorManager.IsMasterEvaluatorFailed());
            Assert.Equal(0, evaluatorManager.NumberofFailedMappers());

            evaluatorManager.ResetFailedEvaluators();
            evaluatorManager.AddAllocatedEvaluator(CreateMockAllocatedEvaluator(1, EvaluatorManager.MasterBatchId + _masterBatchIdSquenceNumber++));
            Assert.True(evaluatorManager.AreAllEvaluatorsAllocated());
        }

        /// <summary>
        /// Test case: no master Evaluator is requested
        /// </summary>
        [Fact]
        public void TestNoMasterEvaluator()
        {
            var evaluatorManager = CreateEvaluatorManager(3, 1);
            evaluatorManager.AddAllocatedEvaluator(CreateMockAllocatedEvaluator(1, EvaluatorManager.MapperBatchId + _mapperBatchIdSquenceNumber++));
            evaluatorManager.AddAllocatedEvaluator(CreateMockAllocatedEvaluator(2, EvaluatorManager.MapperBatchId + _mapperBatchIdSquenceNumber++));
            Action add = () => evaluatorManager.AddAllocatedEvaluator(CreateMockAllocatedEvaluator(3, EvaluatorManager.MapperBatchId + _mapperBatchIdSquenceNumber++));
            Assert.Throws<IMRUSystemException>(add);
        }

        /// <summary>
        /// Test case: request two master Evaluators
        /// </summary>
        [Fact]
        public void TestTwoMasterEvaluator()
        {
            var evaluatorManager = CreateEvaluatorManager(3, 1);
            evaluatorManager.AddAllocatedEvaluator(CreateMockAllocatedEvaluator(1, EvaluatorManager.MasterBatchId + _masterBatchIdSquenceNumber++));
            evaluatorManager.AddAllocatedEvaluator(CreateMockAllocatedEvaluator(2, EvaluatorManager.MapperBatchId + _mapperBatchIdSquenceNumber++));
            Action add = () => evaluatorManager.AddAllocatedEvaluator(CreateMockAllocatedEvaluator(3, EvaluatorManager.MasterBatchId + _masterBatchIdSquenceNumber++));
            Assert.Throws<IMRUSystemException>(add);
        }

        /// <summary>
        /// Test case: number of allocated Evaluators is more than expected number
        /// </summary>
        [Fact]
        public void TestTooManyEvaluators()
        {
            var evaluatorManager = CreateEvaluatorManager(2, 1);
            evaluatorManager.AddAllocatedEvaluator(CreateMockAllocatedEvaluator(1, EvaluatorManager.MasterBatchId + _masterBatchIdSquenceNumber++));
            evaluatorManager.AddAllocatedEvaluator(CreateMockAllocatedEvaluator(2, EvaluatorManager.MapperBatchId + _mapperBatchIdSquenceNumber++));
            Action add = () => evaluatorManager.AddAllocatedEvaluator(CreateMockAllocatedEvaluator(3, EvaluatorManager.MapperBatchId + _mapperBatchIdSquenceNumber++));
            Assert.Throws<IMRUSystemException>(add);
        }

        /// <summary>
        /// Test case: Remove no exists Evaluator
        /// </summary>
        [Fact]
        public void TestRemoveInvalidEvaluators()
        {
            var evaluatorManager = CreateTestEvaluators(3, 1);
            evaluatorManager.RecordFailedEvaluator(EvaluatorIdPrefix + 2);
            Action remove = () => evaluatorManager.RecordFailedEvaluator(EvaluatorIdPrefix + 4);
            Assert.Throws<IMRUSystemException>(remove);
        }

        /// <summary>
        /// Test reset FailedEvaluators
        /// </summary>
        [Fact]
        public void TestResetFailedEvaluators()
        {
            var evaluatorManager = CreateTestEvaluators(3, 1);
            evaluatorManager.RecordFailedEvaluator(EvaluatorIdPrefix + 1);
            evaluatorManager.RecordFailedEvaluator(EvaluatorIdPrefix + 2);
            Assert.Equal(2, evaluatorManager.NumberOfMissingEvaluators());
            evaluatorManager.ResetFailedEvaluators();
            Assert.Equal(0, evaluatorManager.NumberofFailedMappers());
            Assert.False(evaluatorManager.IsMasterEvaluatorId(EvaluatorIdPrefix + 1));
            Assert.False(evaluatorManager.IsMasterEvaluatorFailed());
        }

        /// <summary>
        /// Test case: Maximum number of Evaluator failures has been reached
        /// </summary>
        [Fact]
        public void TestReachedMaximumNumberOfEvaluatorFailures()
        {
            var evaluatorManager = CreateTestEvaluators(3, 1);
            evaluatorManager.RecordFailedEvaluator(EvaluatorIdPrefix + 1);
            evaluatorManager.RecordFailedEvaluator(EvaluatorIdPrefix + 2);
            Assert.True(evaluatorManager.ExceededMaximumNumberOfEvaluatorFailures());
        }

        /// <summary>
        /// Create an EvaluatorManager and add mock Evaluators for testing
        /// </summary>
        /// <param name="totalEvaluators"></param>
        /// <param name="allowedNumberOfEvaluatorFailures"></param>
        /// <returns></returns>
        private EvaluatorManager CreateTestEvaluators(int totalEvaluators, int allowedNumberOfEvaluatorFailures)
        {
            var evaluatorManager = CreateEvaluatorManager(totalEvaluators, allowedNumberOfEvaluatorFailures);
            evaluatorManager.AddAllocatedEvaluator(CreateMockAllocatedEvaluator(1, EvaluatorManager.MasterBatchId + _masterBatchIdSquenceNumber++));
            for (var i = 2; i <= totalEvaluators; i++)
            {
                evaluatorManager.AddAllocatedEvaluator(CreateMockAllocatedEvaluator(i, EvaluatorManager.MapperBatchId + _mapperBatchIdSquenceNumber++));
            }
            return evaluatorManager;
        }

        /// <summary>
        /// Create a mocked IAllocatedEvaluator
        /// </summary>
        /// <param name="id"></param>
        /// <param name="batchId"></param>
        /// <returns></returns>
        private static IAllocatedEvaluator CreateMockAllocatedEvaluator(int id, string batchId)
        {
            IAllocatedEvaluator mockAllocatedEvaluator = Substitute.For<IAllocatedEvaluator>();
            mockAllocatedEvaluator.EvaluatorBatchId.Returns(batchId);
            mockAllocatedEvaluator.Id.Returns(EvaluatorIdPrefix + id);
            return mockAllocatedEvaluator;
        }

        /// <summary>
        /// Create an EvaluatorManager for testing
        /// </summary>
        /// <param name="totalEvaluators"></param>
        /// <param name="allowedNumberOfEvaluatorFailures"></param>
        /// <returns></returns>
        private EvaluatorManager CreateEvaluatorManager(int totalEvaluators, int allowedNumberOfEvaluatorFailures)
        {
            var updateSpec = new EvaluatorSpecification(500, 2);
            var mapperSpec = new EvaluatorSpecification(1000, 3);
            return new EvaluatorManager(totalEvaluators, allowedNumberOfEvaluatorFailures, CreateMockEvaluatorRequestor(), updateSpec, mapperSpec);
        }

        /// <summary>
        /// Create a mock IEvaluatorRequestor
        /// </summary>
        /// <returns></returns>
        private IEvaluatorRequestor CreateMockEvaluatorRequestor()
        {
            return Substitute.For<IEvaluatorRequestor>();
        }
    }
}