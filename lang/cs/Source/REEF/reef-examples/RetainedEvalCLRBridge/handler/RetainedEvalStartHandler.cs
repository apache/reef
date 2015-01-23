/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

using Org.Apache.Reef.Driver;
using Org.Apache.Reef.Driver.Bridge;
using Org.Apache.Reef.Driver.Context;
using Org.Apache.Reef.Driver.Evaluator;
using Org.Apache.Reef.Examples.RetainedEvalBridge;
using Org.Apache.Reef.Examples.RetainedEvalCLRBridge;
using Org.Apache.Reef.Tasks;
using Org.Apache.Reef.Tang.Annotations;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Org.Apache.Reef.Interop.Examples.RetainedEval
{
    public class RetainedEvalStartHandler : IStartHandler
    {
        private static ClrSystemHandler<IEvaluatorRequestor> _evaluatorRequestorHandler;
        private static ClrSystemHandler<IAllocatedEvaluator> _allocatedEvaluatorHandler;
        private static ClrSystemHandler<IActiveContext> _activeContextHandler;

        [Inject]
        public RetainedEvalStartHandler()
        {
            CreateClassHierarchy();
            Identifier = "RetainedEvalStartHandler";
        }

        public RetainedEvalStartHandler(string id)
        {
            Identifier = id;
            CreateClassHierarchy();
        }

        public string Identifier { get; set; }

        public IList<ulong> GetHandlers()
        {
            ulong[] handlers = Enumerable.Repeat(Constants.NullHandler, Constants.HandlersNumber).ToArray();

            // initiate Evaluator Requestor handler
            _evaluatorRequestorHandler = new ClrSystemHandler<IEvaluatorRequestor>();
            handlers[Constants.Handlers[Constants.EvaluatorRequestorHandler]] = ClrHandlerHelper.CreateHandler(_evaluatorRequestorHandler);
            Console.WriteLine("_evaluatorRequestorHandler initiated");
            _evaluatorRequestorHandler.Subscribe(new RetainedEvalEvaluatorRequestorHandler());

            // initiate Allocated Evaluator handler
            _allocatedEvaluatorHandler = new ClrSystemHandler<IAllocatedEvaluator>();
            handlers[Constants.Handlers[Constants.AllocatedEvaluatorHandler]] = ClrHandlerHelper.CreateHandler(_allocatedEvaluatorHandler);
            Console.WriteLine("_allocatedEvaluatorHandler initiated");
            _allocatedEvaluatorHandler.Subscribe(new RetainedEvalAllocatedEvaluatorHandler());

            // initiate Active Context handler
            _activeContextHandler = new ClrSystemHandler<IActiveContext>();
            handlers[Constants.Handlers[Constants.ActiveContextHandler]] = ClrHandlerHelper.CreateHandler(_activeContextHandler);
            Console.WriteLine("_activeContextHandler initiated");
            _activeContextHandler.Subscribe(new RetainedEvalActiveContextHandler());

            return handlers;
        }

        private void CreateClassHierarchy()
        {
            HashSet<string> clrDlls = new HashSet<string>();
            clrDlls.Add(typeof(IDriver).Assembly.GetName().Name);
            clrDlls.Add(typeof(ITask).Assembly.GetName().Name);
            clrDlls.Add(typeof(ShellTask).Assembly.GetName().Name);

            ClrHandlerHelper.GenerateClassHierarchy(clrDlls);
        }
    }
}
