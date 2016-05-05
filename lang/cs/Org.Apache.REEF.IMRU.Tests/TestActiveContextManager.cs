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
using NSubstitute;
using Org.Apache.REEF.Driver.Context;
using Org.Apache.REEF.Driver.Evaluator;
using Org.Apache.REEF.IMRU.OnREEF.Driver;
using Xunit;

namespace Org.Apache.REEF.IMRU.Tests
{
    /// <summary>
    /// Test methods in ActiveContextManager
    /// </summary>
    public class TestActiveContextManager
    {
        private const string EvaluatorIdPrefix = "EvaluatorId";
        private const string ContextIdPrefix = "ContextId";

        /// <summary>
        /// Create a ActiveContextManager and add total number of IActiveContexts to it.
        /// Expect to receive notification in the observer
        /// </summary>
        /// <returns></returns>
        private static ActiveContextManager InitializeActiveContextManager()
        {
            const int totalEvaluators = 5;
            var activeContextManager = new ActiveContextManager(totalEvaluators);
            var contextObserver = new TestContextObserver(totalEvaluators);
            activeContextManager.Subscribe(contextObserver);

            for (int i = 0; i < totalEvaluators; i++)
            {
                activeContextManager.Add(CreateMockActiveContext(i));
            }
            Assert.Equal(totalEvaluators, activeContextManager.NumberOfActiveContexts);
            Assert.Equal(totalEvaluators, contextObserver.NumberOfActiveContextsReceived());

            return activeContextManager;
        }

        /// <summary>
        /// Test add, Remove, RemovedFailedContextInFailedEvaluator, NumberOfMissingContexts and NumberOfActiveContext
        /// in ActiveContexManager
        /// </summary>
        [Fact]
        public void TestValidAddRemoveCases()
        {
            const int totalEvaluators = 3;
            var activeContextManager = InitializeActiveContextManager();
            activeContextManager.RemoveFailedContextInFailedEvaluator(CreateMockFailedEvaluator(new List<int> { totalEvaluators }));
            Assert.Equal(1, activeContextManager.NumberOfMissingContexts);

            activeContextManager.Remove(ContextIdPrefix + 4);
            Assert.Equal(totalEvaluators, activeContextManager.NumberOfActiveContexts);
        }

        /// <summary>
        /// Test invalid Add and Remove
        /// </summary>
        public void TestInvalidAddRemoveCases()
        {
            const int totalEvaluators = 3;
            var activeContextManager = new ActiveContextManager(totalEvaluators);
            activeContextManager.Add(CreateMockActiveContext(1));

            Action add = () => activeContextManager.Add(CreateMockActiveContext(1));
            Assert.Throws<IMRUSystemException>(add);

            Action remove = () => activeContextManager.Remove(ContextIdPrefix + 2);
            Assert.Throws<IMRUSystemException>(remove);

            activeContextManager.Add(CreateMockActiveContext(2));
            activeContextManager.Add(CreateMockActiveContext(3));

            add = () => activeContextManager.Add(CreateMockActiveContext(4));
            Assert.Throws<IMRUSystemException>(add);
        }

        /// <summary>
        /// Test removing a failed evaluator which has two contexts associated.
        /// In current IMRU driver, assume there is only one context associated to the IFailedEvalutor
        /// </summary>
        [Fact]
        public void TestRemoveFailedEvaluatorWithTwoContexts()
        {
            var activeContextManager = InitializeActiveContextManager();

            Action remove = () => activeContextManager.RemoveFailedContextInFailedEvaluator(CreateMockFailedEvaluator(new List<int> { 3, 4 }));
            Assert.Throws<IMRUSystemException>(remove);
        }

        /// <summary>
        /// Test removing a failed evaluator which has a context but it doesn't exist.
        /// </summary>
        [Fact]
        public void TestRemoveFailedEvaluatorWithNoExistsContexts()
        {
            var activeContextManager = InitializeActiveContextManager();

            Action remove = () => activeContextManager.RemoveFailedContextInFailedEvaluator(CreateMockFailedEvaluator(new List<int> { 5 }));
            Assert.Throws<IMRUSystemException>(remove);
        }

        /// <summary>
        /// Test removing a failed evaluator which has no context associated.
        /// The scenario may happen when an evaluator failed but context has not created yet. 
        /// </summary>
        [Fact]
        public void TestRemoveFailedEvaluatorWithNoContext()
        {
            var activeContextManager = InitializeActiveContextManager();

            activeContextManager.RemoveFailedContextInFailedEvaluator(CreateMockFailedEvaluator(null));
            Assert.Equal(0, activeContextManager.NumberOfMissingContexts);
        }

        /// <summary>
        /// Create a mock IActiveContext
        /// </summary>
        /// <param name="id"></param>
        /// <returns></returns>
        private static IActiveContext CreateMockActiveContext(int id)
        {
            IActiveContext mockActiveContext = Substitute.For<IActiveContext>();
            mockActiveContext.Id.Returns(ContextIdPrefix + id);
            mockActiveContext.EvaluatorId.Returns(EvaluatorIdPrefix + ContextIdPrefix + id);
            return mockActiveContext;
        }

        /// <summary>
        /// Create a mock IFailedContext
        /// </summary>
        /// <param name="id"></param>
        /// <returns></returns>
        private static IFailedContext CreateMockFailedContext(int id)
        {
            IFailedContext mockFailedContext = Substitute.For<IFailedContext>();
            mockFailedContext.Id.Returns(ContextIdPrefix + id);
            mockFailedContext.EvaluatorId.Returns(EvaluatorIdPrefix + ContextIdPrefix + id);
            return mockFailedContext;
        }

        /// <summary>
        /// Create a mock IFailedEvaluator
        /// </summary>
        /// <param name="ids"></param>
        /// <returns></returns>
        private static IFailedEvaluator CreateMockFailedEvaluator(IList<int> ids)
        {
            IFailedEvaluator mockFailedEvalutor = Substitute.For<IFailedEvaluator>();
            IList<IFailedContext> failedContexts = null;
            if (ids != null)
            {
                failedContexts = new List<IFailedContext>();
                foreach (var id in ids)
                {
                    failedContexts.Add(CreateMockFailedContext(id));
                }
            }
            mockFailedEvalutor.FailedContexts.Returns(failedContexts);
            return mockFailedEvalutor;
        }

        /// <summary>
        /// A Context Manager observer for test
        /// </summary>
        private sealed class TestContextObserver : IObserver<IDictionary<string, IActiveContext>>
        {
            private readonly int _totalExpected;
            private IDictionary<string, IActiveContext> _contexts = null;

            internal TestContextObserver(int totalExpected)
            {
                _totalExpected = totalExpected;
            }

            public void OnCompleted()
            {
                throw new NotImplementedException();
            }

            public void OnError(Exception error)
            {
                throw new NotImplementedException();
            }

            public int NumberOfActiveContextsReceived()
            {
                if (_contexts != null)
                {
                    return _contexts.Count;                    
                }
                return 0;
            }

            public void OnNext(IDictionary<string, IActiveContext> value)
            {
                _contexts = value;
            }
        }
    }
}