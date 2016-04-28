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
using Org.Apache.REEF.Driver.Context;
using Org.Apache.REEF.Driver.Evaluator;
using Org.Apache.REEF.IMRU.OnREEF.Driver;
using Xunit;

namespace Org.Apache.REEF.IMRU.Tests
{
    /// <summary>
    /// Test methods in ContextManager
    /// </summary>
    public class TestContextManager
    {
        private const string EvaluatorIdPrefix = "EvaluatorId";
        private const string ContextIdPrefix = "ContextId";

        /// <summary>
        /// Test add, Remove, RemovedFailedContextInFailedEvaluator, NumberOfMissingContexts and NumberOfActiveContext
        /// in ContextManager
        /// </summary>
        [Fact]
        public void TestContextmanager()
        {
            var contextManager = new ContextManager(5);
            for (int i = 0; i < 5; i++)
            {
                var c = new MyActiveContext(ContextIdPrefix + i);
                contextManager.Add(c);
            }
            Assert.True(contextManager.AllContextsReceived);
            Assert.Equal(5, contextManager.NumberOfActiveContext);
            var e = new MyFailedEvaluator(ContextIdPrefix + "3");
            contextManager.RemovedFailedContextInFailedEvaluator(e);
            Assert.Equal(1, contextManager.NumberOfMissingContexts);
            contextManager.Remove(ContextIdPrefix + "4");
            Assert.Equal(3, contextManager.NumberOfActiveContext);
        }

        /// <summary>
        /// Test remove a failed evaluator which has two contexts associated.
        /// In current IMRU driver, assume there is only one context associated to the IFailedEvalutor
        /// </summary>
        [Fact]
        public void TestRemoveFailedEvaluatorWithTwoContexts()
        {
            var contextManager = new ContextManager(5);
            for (int i = 0; i < 5; i++)
            {
                var c = new MyActiveContext(ContextIdPrefix + i);
                contextManager.Add(c);
            }

            var e = new MyFailedEvaluatorWithTwoContexts();
            Action remove = () => contextManager.RemovedFailedContextInFailedEvaluator(e);
            Assert.Throws<IMRUSystemException>(remove);
        }

        /// <summary>
        /// Test remove a failed evaluator which has a context but it doesn't exist.
        /// </summary>
        [Fact]
        public void TestRemoveFailedEvaluatorWithNoExistsContexts()
        {
            var contextManager = new ContextManager(5);
            for (int i = 0; i < 5; i++)
            {
                var c = new MyActiveContext(ContextIdPrefix + i);
                contextManager.Add(c);
            }

            var e = new MyFailedEvaluator(ContextIdPrefix + "5");
            Action remove = () => contextManager.RemovedFailedContextInFailedEvaluator(e);
            Assert.Throws<SystemException>(remove);
        }

        /// <summary>
        /// Test remove a failed evaluator which has no context associated.
        /// The scenario may happen when an evaluator failed but context has not created yet. 
        /// </summary>
        [Fact]
        public void TestRemoveFailedEvaluatorWithNoContext()
        {
            var contextManager = new ContextManager(5);
            for (int i = 0; i < 5; i++)
            {
                var c = new MyActiveContext(ContextIdPrefix + i);
                contextManager.Add(c);
            }

            var e = new MyFailedEvaluatorWithNoContext();
            contextManager.RemovedFailedContextInFailedEvaluator(e);
            Assert.Equal(5, contextManager.NumberOfActiveContext);
        }

        /// <summary>
        /// An implementation of IFailedEvaluator for testing
        /// </summary>
        private class MyFailedEvaluator : IFailedEvaluator
        {
            private readonly string _contextId;

            /// <summary>
            /// Constructor of MyFailedEvaluator
            /// The contextId is used in the associated IFailedContext 
            /// </summary>
            /// <param name="contextId"></param>
            internal MyFailedEvaluator(string contextId)
            {
                _contextId = contextId;
            }

            public EvaluatorException EvaluatorException
            {
                get { throw new NotImplementedException(); }
            }

            /// <summary>
            /// Have one failed context in the failed Evaluator
            /// </summary>
            public IList<IFailedContext> FailedContexts
            {
                get
                {
                    IList<IFailedContext> contexts = new List<IFailedContext>();
                    contexts.Add(new MyFailedContext(_contextId));
                    return contexts;
                }
            }

            public Utilities.Optional<Driver.Task.IFailedTask> FailedTask
            {
                get { throw new NotImplementedException(); }
            }

            /// <summary>
            /// Returns Evaluator id
            /// </summary>
            public string Id
            {
                get { return EvaluatorIdPrefix + _contextId; }
            }
        }

        /// <summary>
        /// An implementation of IFailedEvaluator for testing
        /// with no context associated
        /// </summary>
        private class MyFailedEvaluatorWithNoContext : IFailedEvaluator
        {
            public EvaluatorException EvaluatorException
            {
                get { throw new NotImplementedException(); }
            }

            /// <summary>
            /// Returns null FailedContexts
            /// </summary>
            public IList<IFailedContext> FailedContexts
            {
                get { return null; }
            }

            public Utilities.Optional<Driver.Task.IFailedTask> FailedTask
            {
                get { throw new NotImplementedException(); }
            }

            /// <summary>
            /// Returns Evaluator id
            /// </summary>
            public string Id
            {
                get { return EvaluatorIdPrefix + "no"; }
            }
        }

        /// <summary>
        /// An implementation of IFailedEvaluator for testing
        /// with two contexts associated
        /// </summary>
        private class MyFailedEvaluatorWithTwoContexts : IFailedEvaluator
        {
            public EvaluatorException EvaluatorException
            {
                get { throw new NotImplementedException(); }
            }

            /// <summary>
            /// Returns two FailedContexts
            /// </summary>
            public IList<IFailedContext> FailedContexts
            {
                get
                {
                    IList<IFailedContext> contexts = new List<IFailedContext>();
                    contexts.Add(new MyFailedContext(ContextIdPrefix + "1"));
                    contexts.Add(new MyFailedContext(ContextIdPrefix + "2"));
                    return contexts;
                }
            }

            public Utilities.Optional<Driver.Task.IFailedTask> FailedTask
            {
                get { throw new NotImplementedException(); }
            }

            /// <summary>
            /// Returns Evaluator id
            /// </summary>
            public string Id
            {
                get { return EvaluatorIdPrefix + "no"; }
            }
        }

        private class MyFailedContext : IFailedContext
        {
            private readonly string _id;

            internal MyFailedContext(string id)
            {
                _id = id;
            }

            public Utilities.Optional<IActiveContext> ParentContext
            {
                get { throw new NotImplementedException(); }
            }

            public string EvaluatorId
            {
                get { return EvaluatorIdPrefix + _id; }
            }

            public Utilities.Optional<string> ParentId
            {
                get { throw new NotImplementedException(); }
            }

            public IEvaluatorDescriptor EvaluatorDescriptor
            {
                get { throw new NotImplementedException(); }
            }

            public string Id
            {
                get { return _id; }
            }
        }

        /// <summary>
        /// An implementation of IActiveContext for testing
        /// </summary>
        private sealed class MyActiveContext : IActiveContext
        {
            private readonly string _id;

            /// <summary>
            /// Constructor which gets id of MyActiveContext
            /// </summary>
            /// <param name="id"></param>
            internal MyActiveContext(string id)
            {
                _id = id;
            }

            public void SendMessage(byte[] message)
            {
                throw new NotImplementedException();
            }

            public void Dispose()
            {
                throw new NotImplementedException();
            }

            /// <summary>
            /// Returns Evaluator Id of the MyActiveContext
            /// </summary>
            public string EvaluatorId
            {
                get { return EvaluatorIdPrefix + _id; }
            }

            public Utilities.Optional<string> ParentId
            {
                get { throw new NotImplementedException(); }
            }

            public IEvaluatorDescriptor EvaluatorDescriptor
            {
                get { throw new NotImplementedException(); }
            }

            /// <summary>
            /// Returns Id of the Active Context
            /// </summary>
            public string Id
            {
                get { return _id; }
            }

            public void SubmitTask(Tang.Interface.IConfiguration taskConf)
            {
                throw new NotImplementedException();
            }

            public void SubmitContext(Tang.Interface.IConfiguration contextConfiguration)
            {
                throw new NotImplementedException();
            }

            public void SubmitContextAndService(Tang.Interface.IConfiguration contextConfiguration, Tang.Interface.IConfiguration serviceConfiguration)
            {
                throw new NotImplementedException();
            }
        }
    }
}