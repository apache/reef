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
using Xunit;
using Org.Apache.REEF.Driver.Evaluator;
using Org.Apache.REEF.IMRU.OnREEF.Driver;

namespace Org.Apache.REEF.IMRU.Tests
{
    /// <summary>
    /// Test cases for EvaluatorManager
    /// </summary>
    public sealed class TestEvaluatorManager
    {
        private const string EvaluatorIdPrefix = "EvaluatorId";

        /// <summary>
        /// Test valid add, remove Evaluators
        /// </summary>
        [Fact]
        public void TestValidAddRemoveAllocatedEvaluator()
        {
            var evalutorManager = CreateTestEvaluators(3, 1);
            Assert.Equal(3, evalutorManager.NumberOfAllocatedEvaluators);
            Assert.True(evalutorManager.AreAllEvaluatorsAllocated());
            Assert.True(evalutorManager.IsMasterEvaluatorId(EvaluatorIdPrefix + 1));
            Assert.False(evalutorManager.IsMasterEvaluatorId(EvaluatorIdPrefix + 2));
            Assert.True(evalutorManager.IsAllocatedEvaluator(EvaluatorIdPrefix + 2));
            Assert.False(evalutorManager.IsMasterEvaluatorFailed());

            evalutorManager.RecordFailedEvaluator(EvaluatorIdPrefix + 1);
            Assert.Equal(2, evalutorManager.NumberOfAllocatedEvaluators);
            Assert.True(evalutorManager.IsMasterEvaluatorFailed());
            Assert.Equal(0, evalutorManager.NumberofFailedMappers());

            evalutorManager.ResetFailedEvaluators();
            evalutorManager.AddAllocatedEvaluator(CreateMockAllocatedEvaluator(1, EvaluatorManager.MasterBatchId));
            Assert.True(evalutorManager.AreAllEvaluatorsAllocated());
        }

        /// <summary>
        /// Test case: no master Evaluator is requested
        /// </summary>
        [Fact]
        public void TestNoMasterEvaluator()
        {
            var evalutorManager = CreateEvaluatorManager(3, 1);
            evalutorManager.AddAllocatedEvaluator(CreateMockAllocatedEvaluator(1, EvaluatorManager.MapperBatchId));
            evalutorManager.AddAllocatedEvaluator(CreateMockAllocatedEvaluator(2, EvaluatorManager.MapperBatchId));
            Action add = () => evalutorManager.AddAllocatedEvaluator(CreateMockAllocatedEvaluator(3, EvaluatorManager.MapperBatchId));
            Assert.Throws<IMRUSystemException>(add);
        }

        /// <summary>
        /// Test case: request two master Evaluators
        /// </summary>
        [Fact]
        public void TestTwoMasterEvaluator()
        {
            var evalutorManager = CreateEvaluatorManager(3, 1);
            evalutorManager.AddAllocatedEvaluator(CreateMockAllocatedEvaluator(1, EvaluatorManager.MasterBatchId));
            evalutorManager.AddAllocatedEvaluator(CreateMockAllocatedEvaluator(2, EvaluatorManager.MapperBatchId));
            Action add = () => evalutorManager.AddAllocatedEvaluator(CreateMockAllocatedEvaluator(3, EvaluatorManager.MasterBatchId));
            Assert.Throws<IMRUSystemException>(add);
        }

        /// <summary>
        /// Test case: number of allocated Evaluators is more than expected number
        /// </summary>
        [Fact]
        public void TestTooManyEvaluators()
        {
            var evalutorManager = CreateEvaluatorManager(2, 1);
            evalutorManager.AddAllocatedEvaluator(CreateMockAllocatedEvaluator(1, EvaluatorManager.MasterBatchId));
            evalutorManager.AddAllocatedEvaluator(CreateMockAllocatedEvaluator(2, EvaluatorManager.MapperBatchId));
            Action add = () => evalutorManager.AddAllocatedEvaluator(CreateMockAllocatedEvaluator(3, EvaluatorManager.MapperBatchId));
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
            var evalutorManager = CreateTestEvaluators(3, 1);
            evalutorManager.RecordFailedEvaluator(EvaluatorIdPrefix + 1);
            evalutorManager.RecordFailedEvaluator(EvaluatorIdPrefix + 2);
            Assert.Equal(2, evalutorManager.NumberOfMissingEvaluators());
            evalutorManager.ResetFailedEvaluators();
            Assert.Equal(0, evalutorManager.NumberofFailedMappers());
            Assert.False(evalutorManager.IsMasterEvaluatorId(EvaluatorIdPrefix + 1));
            Assert.False(evalutorManager.IsMasterEvaluatorFailed());
        }

        /// <summary>
        /// Test case: Maximum number of Evaluator failures has been reached
        /// </summary>
        [Fact]
        public void TestReachedMaximumNumberOfEvaluatorFailures()
        {
            var evalutorManager = CreateTestEvaluators(3, 2);
            evalutorManager.RecordFailedEvaluator(EvaluatorIdPrefix + 1);
            evalutorManager.RecordFailedEvaluator(EvaluatorIdPrefix + 2);
            Assert.True(evalutorManager.ReachedMaximumNumberOfEvaluatorFailures());
        }

        /// <summary>
        /// Create a an EvaluatorManager and add mock Evaluators for testing
        /// </summary>
        /// <param name="totalEvaluators"></param>
        /// <param name="allowedNumberOfEvaluatorFailures"></param>
        /// <returns></returns>
        private EvaluatorManager CreateTestEvaluators(int totalEvaluators, int allowedNumberOfEvaluatorFailures)
        {
            var evaluatorManager = CreateEvaluatorManager(totalEvaluators, allowedNumberOfEvaluatorFailures);
            evaluatorManager.AddAllocatedEvaluator(CreateMockAllocatedEvaluator(1, EvaluatorManager.MasterBatchId));
            for (var i = 2; i <= totalEvaluators; i++)
            {
                evaluatorManager.AddAllocatedEvaluator(CreateMockAllocatedEvaluator(i, EvaluatorManager.MapperBatchId));
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