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

using Org.Apache.Reef.Common;
using Org.Apache.Reef.Common.Api;
using Org.Apache.Reef.Common.Catalog;
using Org.Apache.Reef.Common.Evaluator;
using Org.Apache.Reef.Common.Exceptions;
using Org.Apache.Reef.Common.ProtoBuf.DriverRuntimeProto;
using Org.Apache.Reef.Common.ProtoBuf.EvaluatorRunTimeProto;
using Org.Apache.Reef.Common.ProtoBuf.ReefProtocol;
using Org.Apache.Reef.Common.ProtoBuf.ReefServiceProto;
using Org.Apache.Reef.Driver.Bridge;
using Org.Apache.Reef.Driver.Evaluator;
using Org.Apache.Reef.Utilities;
using Org.Apache.Reef.Utilities.Diagnostics;
using Org.Apache.Reef.Utilities.Logging;
using Org.Apache.Reef.Tang.Implementations;
using Org.Apache.Reef.Tang.Interface;
using Org.Apache.Reef.Wake.Remote;
using Org.Apache.Reef.Wake.Time;
using Org.Apache.Reef.Wake.Time.Runtime.Event;
using System;
using System.Collections.Generic;
using System.Globalization;

namespace Org.Apache.Reef.Driver
{
    public class DriverManager : 
        IEvaluatorRequestor, 
        IObserver<RuntimeStatusProto>, 
        IObserver<ResourceStatusProto>,
        IObserver<ResourceAllocationProto>,
        IObserver<NodeDescriptorProto>,
        IObserver<RuntimeStart>,
        IObserver<RuntimeStop>,
        IObserver<IdleClock>
    {
        private static readonly Logger LOGGER = Logger.GetLogger(typeof(DriverManager));
        
        private IInjector _injector;

        private IInjectionFuture<IClock> _clockFuture; 

        private ResourceCatalogImpl _resourceCatalog;

        private IInjectionFuture<IResourceRequestHandler> _futureResourceRequestHandler;
        
        private Dictionary<string, EvaluatorManager> _evaluators = new Dictionary<string, EvaluatorManager>();

        private EvaluatorHeartBeatSanityChecker _sanityChecker = new EvaluatorHeartBeatSanityChecker();

        private ClientJobStatusHandler _clientJobStatusHandler;

        private IDisposable _heartbeatConnectionChannel;

        private IDisposable _errorChannel;

        private IObserver<RuntimeErrorProto> _runtimeErrorHandler;

        public DriverManager(
            IInjector injector,
            ResourceCatalogImpl resourceCatalog,
            IRemoteManager<REEFMessage> remoteManager,
            IInjectionFuture<IClock> clockFuture,
            IInjectionFuture<IResourceRequestHandler> futureResourceRequestHandler,
            ClientJobStatusHandler clientJobStatusHandler,
            string clientRId)
        {
            _injector = injector;
            _clockFuture = clockFuture;
            _resourceCatalog = resourceCatalog;
            _futureResourceRequestHandler = futureResourceRequestHandler;
            _clientJobStatusHandler = clientJobStatusHandler;

            _heartbeatConnectionChannel = null;
            _errorChannel = null;
            _runtimeErrorHandler = null;
            LOGGER.Log(Level.Info, "DriverManager instantiated");
        }

        public IResourceCatalog ResourceCatalog
        {
            get
            {
                return _resourceCatalog;
            }

            set
            {
            }
        }

        private RuntimeStatusProto _runtimeStatusProto
        {
            get
            {
                RuntimeStatusProto proto = new RuntimeStatusProto();
                proto.state = State.INIT;
                proto.name = "REEF";
                proto.outstanding_container_requests = 0;
                return proto;
            }

            set
            {
                _runtimeStatusProto = value;
            }
        }

        public void Submit(IEvaluatorRequest request)
        {
            LOGGER.Log(Level.Info, "Got an EvaluatorRequest");
            ResourceRequestProto proto = new ResourceRequestProto();
            //TODO: request.size deprecated should use megabytes instead
            //switch (request.Size)
            //{
            //        case EvaluatorRequest.EvaluatorSize.SMALL:
            //        proto.resource_size = SIZE.SMALL;
            //        break;
            //        case EvaluatorRequest.EvaluatorSize.MEDIUM:
            //        proto.resource_size = SIZE.MEDIUM;
            //        break;
            //        case EvaluatorRequest.EvaluatorSize.LARGE:
            //        proto.resource_size = SIZE.LARGE;
            //        break;
            //        case EvaluatorRequest.EvaluatorSize.XLARGE:
            //        proto.resource_size = SIZE.XLARGE;
            //        break;
            //    default:
            //        throw new InvalidOperationException("invalid request size" + request.Size);
            //}
            proto.resource_count = request.Number;
            if (request.MemoryMegaBytes > 0)
            {
                proto.memory_size = request.MemoryMegaBytes;
            }

            //final ResourceCatalog.Descriptor descriptor = req.getDescriptor();
            //if (descriptor != null) {
            //  if (descriptor instanceof RackDescriptor) {
            //    request.addRackName(descriptor.getName());
            //  } else if (descriptor instanceof NodeDescriptor) {
            //    request.addNodeName(descriptor.getName());
            //  }
            //}

            //_futureResourceRequestHandler.Get().OnNext(proto);
        }

        public void Release(EvaluatorManager evaluatorManager)
        {
            lock (this)
            {
                string evaluatorManagerId = evaluatorManager.Id;
                if (_evaluators.ContainsKey(evaluatorManagerId))
                {
                    _evaluators.Remove(evaluatorManagerId);
                }
                else
                {
                    var e = new InvalidOperationException("Trying to remove an unknown evaluator manager with id " + evaluatorManagerId);
                    Exceptions.Throw(e, LOGGER);
                }
            }
        }

        /// <summary>
        /// This handles runtime error occurs on the evaluator
        /// </summary>
        /// <param name="runtimeErrorProto"></param>
        public void Handle(RuntimeErrorProto runtimeErrorProto)
        {
            FailedRuntime error = new FailedRuntime(runtimeErrorProto);
            LOGGER.Log(Level.Warning, "Runtime error:" + error);

            EvaluatorException evaluatorException = error.Cause != null
                ? new EvaluatorException(error.Id, error.Cause.Value)
                : new EvaluatorException(error.Id, "Runtime error");
            EvaluatorManager evaluatorManager = null;
            lock (_evaluators)
            {
                if (_evaluators.ContainsKey(error.Id))
                {
                    evaluatorManager = _evaluators[error.Id];
                }
                else
                {
                    LOGGER.Log(Level.Warning, "Unknown evaluator runtime error: " + error.Cause);
                }
            }
            if (null != evaluatorManager)
            {
                evaluatorManager.Handle(evaluatorException);
            }
        }

        /// <summary>
        /// A RuntimeStatusProto comes from the ResourceManager layer indicating its current status
        /// </summary>
        /// <param name="runtimeStatusProto"></param>
        public void OnNext(RuntimeStatusProto runtimeStatusProto)
        {
            Handle(runtimeStatusProto);
        }

        /// <summary>
        /// A ResourceStatusProto message comes from the ResourceManager layer to indicate what it thinks
        /// about the current state of a given resource. Ideally, we should think the same thing.
        /// </summary>
        /// <param name="resourceStatusProto"></param>
        public void OnNext(ResourceStatusProto resourceStatusProto)
        {
            Handle(resourceStatusProto);
        }

        /// <summary>
        /// A ResourceAllocationProto indicates a resource allocation given by the ResourceManager layer.
        /// </summary>
        /// <param name="resourceAllocationProto"></param>
        public void OnNext(ResourceAllocationProto resourceAllocationProto)
        {
            Handle(resourceAllocationProto);
        }

        /// <summary>
        ///  A NodeDescriptorProto defines a new node in the cluster. We should add this to the resource catalog
        /// so that clients can make resource requests against it.
        /// </summary>
        /// <param name="nodeDescriptorProto"></param>
        public void OnNext(NodeDescriptorProto nodeDescriptorProto)
        {
            _resourceCatalog.Handle(nodeDescriptorProto);
        }

        /// <summary>
        /// This EventHandler is subscribed to the StartTime event of the Clock statically. It therefore provides the entrance
        /// point to REEF.
        /// </summary>
        /// <param name="runtimeStart"></param>
        public void OnNext(RuntimeStart runtimeStart)
        {
            LOGGER.Log(Level.Info, "RuntimeStart: " + runtimeStart);
            _runtimeStatusProto = new RuntimeStatusProto();
            _runtimeStatusProto.state = State.RUNNING;
            _runtimeStatusProto.name = "REEF";
            _runtimeStatusProto.outstanding_container_requests = 0;
        }

        /// <summary>
        /// Handles RuntimeStop
        /// </summary>
        /// <param name="runtimeStop"></param>
        public void OnNext(RuntimeStop runtimeStop)
        {
            LOGGER.Log(Level.Info, "RuntimeStop: " + runtimeStop);
            if (runtimeStop.Exception != null)
            {
                string exceptionMessage = runtimeStop.Exception.Message;
                LOGGER.Log(Level.Warning, "Sending runtime error:" + exceptionMessage);
                RuntimeErrorProto runtimeErrorProto = new RuntimeErrorProto();
                runtimeErrorProto.message = exceptionMessage;
                runtimeErrorProto.exception = ByteUtilities.StringToByteArrays(exceptionMessage);
                runtimeErrorProto.name = "REEF";
                _runtimeErrorHandler.OnNext(runtimeErrorProto);

                LOGGER.Log(Level.Warning, "DONE Sending runtime error: " + exceptionMessage);
            }

            lock (_evaluators)
            {
                foreach (EvaluatorManager evaluatorManager in _evaluators.Values)
                {
                    LOGGER.Log(Level.Warning, "Unclean shutdown of evaluator: " + evaluatorManager.Id);
                    evaluatorManager.Dispose();
                }
            }

            try
            {
                _heartbeatConnectionChannel.Dispose();
                _errorChannel.Dispose();
                Optional<Exception> e = runtimeStop.Exception != null ?
                    Optional<Exception>.Of(runtimeStop.Exception) : Optional<Exception>.Empty();
                _clientJobStatusHandler.Dispose(e);

                LOGGER.Log(Level.Info, "driver manager closed");
            }
            catch (Exception e)
            {
                Exceptions.Caught(e, Level.Error, "Error disposing Driver manager", LOGGER);
                Exceptions.Throw(new InvalidOperationException("Cannot dispose driver manager"), LOGGER);
            }
        }

        public void OnNext(IdleClock value)
        {
            string message = string.Format(
                CultureInfo.InvariantCulture,
                "IdleClock: [{0}], RuntimeState [{1}], Outstanding container requests [{2}], Container allocation count[{3}]",
                value + Environment.NewLine,
                _runtimeStatusProto.state + Environment.NewLine,
                _runtimeStatusProto.outstanding_container_requests + Environment.NewLine,
                _runtimeStatusProto.container_allocation.Count);
            LOGGER.Log(Level.Info, message);

            lock (_evaluators)
            {
                if (_runtimeStatusProto.state == State.RUNNING
                    && _runtimeStatusProto.outstanding_container_requests == 0
                    && _runtimeStatusProto.container_allocation.Count == 0)
                {
                    LOGGER.Log(Level.Info, "Idle runtime shutdown");
                    _clockFuture.Get().Dispose();
                }
            }
        }

        void IObserver<IdleClock>.OnError(Exception error)
        {
            throw new NotImplementedException();
        }

        void IObserver<IdleClock>.OnCompleted()
        {
            throw new NotImplementedException();
        }

        void IObserver<RuntimeStop>.OnError(Exception error)
        {
            throw new NotImplementedException();
        }

        void IObserver<RuntimeStop>.OnCompleted()
        {
            throw new NotImplementedException();
        }

        void IObserver<RuntimeStart>.OnError(Exception error)
        {
            throw new NotImplementedException();
        }

        void IObserver<RuntimeStart>.OnCompleted()
        {
            throw new NotImplementedException();
        }

        void IObserver<NodeDescriptorProto>.OnError(Exception error)
        {
            throw new NotImplementedException();
        }

        void IObserver<NodeDescriptorProto>.OnCompleted()
        {
            throw new NotImplementedException();
        }

        void IObserver<ResourceAllocationProto>.OnError(Exception error)
        {
            throw new NotImplementedException();
        }

        void IObserver<ResourceAllocationProto>.OnCompleted()
        {
            throw new NotImplementedException();
        }

        void IObserver<ResourceStatusProto>.OnError(Exception error)
        {
            throw new NotImplementedException();
        }

        void IObserver<ResourceStatusProto>.OnCompleted()
        {
            throw new NotImplementedException();
        }

        void IObserver<RuntimeStatusProto>.OnError(Exception error)
        {
            throw new NotImplementedException();
        }

        void IObserver<RuntimeStatusProto>.OnCompleted()
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Something went wrong at the runtime layer (either driver or evaluator). This
        /// method simply forwards the RuntimeErrorProto to the client via the RuntimeErrorHandler.
        /// </summary>
        /// <param name="runtimeErrorProto"></param>
        private void Fail(RuntimeErrorProto runtimeErrorProto)
        {
            _runtimeErrorHandler.OnNext(runtimeErrorProto);
            _clockFuture.Get().Dispose();
        }

        /// <summary>
        ///  Helper method to create a new EvaluatorManager instance
        /// </summary>
        /// <param name="id">identifier of the Evaluator</param>
        /// <param name="descriptor"> NodeDescriptor on which the Evaluator executes.</param>
        /// <returns>new EvaluatorManager instance.</returns>
        private EvaluatorManager GetNewEvaluatorManagerInstance(string id, EvaluatorDescriptorImpl descriptor)
        {
            LOGGER.Log(Level.Info, "Creating Evaluator Manager: " + id);
            //TODO bindVolatieParameter
            return (EvaluatorManager)_injector.GetInstance(typeof(EvaluatorManager));
        }

        /// <summary>
        ///  Receives and routes heartbeats from Evaluators.
        /// </summary>
        /// <param name="evaluatorHearBeatProto"></param>
        private void Handle(IRemoteMessage<EvaluatorHeartbeatProto> evaluatorHearBeatProto)
        {
            EvaluatorHeartbeatProto heartbeat = evaluatorHearBeatProto.Message;
            EvaluatorStatusProto status = heartbeat.evaluator_status;
            string evaluatorId = status.evaluator_id;
            LOGGER.Log(Level.Info, string.Format(CultureInfo.InvariantCulture, "Heartbeat from Evaluator {0} with state {1} timestamp {2}", evaluatorId, status.state, heartbeat.timestamp));
            _sanityChecker.check(evaluatorId, heartbeat.timestamp);

            lock (_evaluators)
            {
                if (_evaluators.ContainsKey(evaluatorId))
                {
                    EvaluatorManager evaluatorManager = _evaluators[evaluatorId];
                    evaluatorManager.Handle(evaluatorHearBeatProto);
                }
                else
                {
                    string msg = "Contact from unkonwn evaluator with id: " + evaluatorId;
                    if (heartbeat.evaluator_status != null)
                    {
                        msg += " with state" + status.state;
                    }
                    LOGGER.Log(Level.Error, msg);
                    Exceptions.Throw(new InvalidOperationException(msg), LOGGER);
                }
            }            
        }

        /// <summary>
        /// This resource status message comes from the ResourceManager layer; telling me what it thinks
        /// about the state of the resource executing an Evaluator; This method simply passes the message
        /// off to the referenced EvaluatorManager
        /// </summary>
        /// <param name="resourceStatusProto"></param>
        private void Handle(ResourceStatusProto resourceStatusProto)
        {
            lock (_evaluators)
            {
                if (_evaluators.ContainsKey(resourceStatusProto.identifier))
                {
                    EvaluatorManager evaluatorManager = _evaluators[resourceStatusProto.identifier];
                    evaluatorManager.Handle(resourceStatusProto);
                }
                else
                {
                    var e = new InvalidOperationException(string.Format(CultureInfo.InvariantCulture, "Unknown resource status from evaluator {0} with state {1}", resourceStatusProto.identifier, resourceStatusProto.state));
                    Exceptions.Throw(e, LOGGER);
                }
            }
        }

        /// <summary>
        ///  This method handles resource allocations by creating a new EvaluatorManager instance.
        /// </summary>
        /// <param name="resourceAllocationProto"></param>
        private void Handle(ResourceAllocationProto resourceAllocationProto)
        {
            lock (_evaluators)
            {
                try
                {
                    INodeDescriptor nodeDescriptor = _resourceCatalog.GetNode(resourceAllocationProto.node_id);
                    if (nodeDescriptor == null)
                    {
                        Exceptions.Throw(new InvalidOperationException("Unknown resurce: " + resourceAllocationProto.node_id), LOGGER);
                    }
                    EvaluatorDescriptorImpl evaluatorDescriptor = new EvaluatorDescriptorImpl(nodeDescriptor, EvaluatorType.UNDECIDED, resourceAllocationProto.resource_memory, resourceAllocationProto.virtual_cores);
                    LOGGER.Log(Level.Info, "Resource allocation: new evaluator id: " + resourceAllocationProto.identifier);
                    EvaluatorManager evaluatorManager = GetNewEvaluatorManagerInstance(resourceAllocationProto.identifier, evaluatorDescriptor);
                    _evaluators.Add(resourceAllocationProto.identifier, evaluatorManager);
                }
                catch (Exception e)
                {
                    Exceptions.Caught(e, Level.Error, LOGGER);
                    Exceptions.Throw(new InvalidOperationException("Error handling resourceAllocationProto."), LOGGER);
                }
            }
        }

        private void Handle(RuntimeStatusProto runtimeStatusProto)
        {
            State runtimeState = runtimeStatusProto.state;
            LOGGER.Log(Level.Info, "Runtime status: " + runtimeStatusProto.state);

            switch (runtimeState)
            {
                case State.FAILED:
                    Fail(runtimeStatusProto.error);
                    break;
                case State.DONE:
                    _clockFuture.Get().Dispose();
                    break;
                case State.RUNNING:
                    lock (_evaluators)
                    {
                        _runtimeStatusProto = runtimeStatusProto;
                        if (_clockFuture.Get().IsIdle()
                            && runtimeStatusProto.outstanding_container_requests == 0
                            && runtimeStatusProto.container_allocation.Count == 0)
                        {
                            _clockFuture.Get().Dispose();
                        }
                    }
                    break;
            }
        }
    }
}
