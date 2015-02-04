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

using Org.Apache.Reef.Common.ProtoBuf.EvaluatorRunTimeProto;
using Org.Apache.Reef.Driver.Bridge;
using Org.Apache.Reef.Driver.Context;
using Org.Apache.Reef.Driver.Evaluator;
using Org.Apache.Reef.Utilities;
using Org.Apache.Reef.Utilities.Diagnostics;
using Org.Apache.Reef.Utilities.Logging;
using Org.Apache.Reef.Tang.Interface;
using System;
using System.Globalization;

namespace Org.Apache.Reef.Driver
{
    public class EvaluatorContext : IActiveContext
    {
        private static readonly Logger LOGGER = Logger.GetLogger(typeof(EvaluatorContext));
        
        private string _identifier;

        private Optional<string> _parentId;

        private EvaluatorManager _evaluatorManager;

        private bool _disposed = false;

        public EvaluatorContext(EvaluatorManager evaluatorManager, string id, Optional<string> parentId)
        {
            _identifier = id;
            _parentId = parentId;
            _evaluatorManager = evaluatorManager;
        }

        public string Id
        {
            get
            {
                return _identifier;
            }

            set
            {
            }
        }

        public string EvaluatorId
        {
            get
            {
                return _evaluatorManager.Id;
            }

            set
            {
            }
        }

        public Optional<string> ParentId
        {
            get
            {
                return _parentId;
            }

            set
            {
            }
        }

        public IEvaluatorDescriptor EvaluatorDescriptor
        {
            get
            {
                return _evaluatorManager.EvaluatorDescriptor;
            }

            set
            {
            }
        }

        public void Dispose()
        {
            if (_disposed)
            {
                var e = new InvalidOperationException(string.Format(CultureInfo.InvariantCulture, "Active context [{0}] already closed", _identifier));
                Exceptions.Throw(e, LOGGER);
            }
            LOGGER.Log(Level.Info, string.Format(CultureInfo.InvariantCulture, "Submit close context: RunningEvaluator id [{0}] for context id [{1}]", EvaluatorId, Id));
            RemoveContextProto removeContextProto = new RemoveContextProto();
            removeContextProto.context_id = Id;
            ContextControlProto contextControlProto = new ContextControlProto();
            contextControlProto.remove_context = removeContextProto;
            _evaluatorManager.Handle(contextControlProto);
            _disposed = true;
        }

        public ClosedContext GetClosedContext(IActiveContext parentContext)
        {
            //return new ClosedContext(parentContext, EvaluatorId, Id, ParentId, EvaluatorDescriptor);
            throw new NotImplementedException();
        }

        public FailedContext GetFailedContext(Optional<IActiveContext> parentContext, Exception cause)
        {
            //return new FailedContext(parentContext, Id, cause, EvaluatorId, ParentId, EvaluatorDescriptor);
            throw new NotImplementedException();
        }

        public void SubmitTask(IConfiguration taskConf)
        {
            throw new NotImplementedException();
        }

        public void SubmitContext(IConfiguration contextConfiguration)
        {
            throw new NotImplementedException();
        }

        public void SubmitContextAndService(IConfiguration contextConfiguration, IConfiguration serviceConfiguration)
        {
            throw new NotImplementedException();
        }

        public void SendMessage(byte[] message)
        {
            throw new NotImplementedException();
        }
    }
}
