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
using System.Globalization;
using System.Runtime.Serialization;
using Org.Apache.REEF.Common.Exceptions;
using Org.Apache.REEF.Common.Protobuf.ReefProtocol;
using Org.Apache.REEF.Common.Runtime.Evaluator.Context;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Exceptions;
using Org.Apache.REEF.Utilities;
using Org.Apache.REEF.Utilities.Diagnostics;
using Org.Apache.REEF.Utilities.Logging;
using Org.Apache.REEF.Wake.Time;
using Org.Apache.REEF.Wake.Time.Runtime.Event;

namespace Org.Apache.REEF.Common.Runtime.Evaluator
{
    internal sealed class EvaluatorRuntime : IObserver<RuntimeStart>, IObserver<RuntimeStop>, IObserver<REEFMessage>
    {
        private static readonly Logger Logger = Logger.GetLogger(typeof(EvaluatorRuntime));
        
        private readonly string _evaluatorId;
        private readonly ContextManager _contextManager;
        private readonly IHeartBeatManager _heartBeatManager;
        private readonly IClock _clock;
        private readonly IDisposable _evaluatorControlChannel; 
        private readonly PIDStoreHandler _pidStoreHelper;
        private readonly EvaluatorExitLogger _evaluatorExitLogger;

        private State _state = State.INIT;

        [Inject]
        public EvaluatorRuntime(
            ContextManager contextManager,
            IHeartBeatManager heartBeatManager,
            PIDStoreHandler pidStoreHelper,
            EvaluatorExitLogger evaluatorExitLogger)
        {
            using (Logger.LogFunction("EvaluatorRuntime::EvaluatorRuntime"))
            {
                _heartBeatManager = heartBeatManager;
                _clock = _heartBeatManager.EvaluatorSettings.RuntimeClock;
                _contextManager = contextManager;
                _evaluatorId = _heartBeatManager.EvaluatorSettings.EvalutorId;
                _pidStoreHelper = pidStoreHelper;
                _evaluatorExitLogger = evaluatorExitLogger;

                var remoteManager = _heartBeatManager.EvaluatorSettings.RemoteManager;

                ReefMessageProtoObserver driverObserver = new ReefMessageProtoObserver();

                // subscribe to driver proto message
                driverObserver.Subscribe(o => OnNext(o.Message));

                // register the driver observer
                _evaluatorControlChannel = remoteManager.RegisterObserver(driverObserver);

                AppDomain.CurrentDomain.UnhandledException += EvaluatorRuntimeUnhandledException;
                DefaultUnhandledExceptionHandler.Unregister();

                // start the heart beat
                _clock.ScheduleAlarm(0, _heartBeatManager);
            }
        }

        private void EvaluatorRuntimeUnhandledException(object sender, UnhandledExceptionEventArgs e)
        {
            OnException((Exception)e.ExceptionObject);
        }

        public State State
        {
            get
            {
                return _state;
            }
        }

        private string MessageFieldAsText(object field)
        {
            return field == null ? "null" : "not null";
        }

        public void Handle(EvaluatorControlProto message)
        {
            lock (_heartBeatManager)
            {
                var msg = " done_evaluator = " + MessageFieldAsText(message.done_evaluator)
                          + " kill_evaluator = " + MessageFieldAsText(message.kill_evaluator)
                          + " stop_evaluator = " + MessageFieldAsText(message.stop_evaluator)
                          + " context_control = " + MessageFieldAsText(message.context_control);
                Logger.Log(Level.Info, "Handle Evaluator control message: " + msg);
                if (!message.identifier.Equals(_evaluatorId, StringComparison.OrdinalIgnoreCase))
                {
                    OnException(new InvalidOperationException(
                        string.Format(CultureInfo.InvariantCulture, "Identifier mismatch: message for evaluator id[{0}] sent to evaluator id[{1}]", message.identifier, _evaluatorId)));
                }
                else if (_state == State.DONE)
                {
                    if (message.done_evaluator != null)
                    {
                        Logger.Log(Level.Info, "Received ACK from Driver, shutting down Evaluator.");
                        _clock.Dispose();

                        return;
                    }
                    else
                    {
                        OnException(new InvalidOperationException("Received a control message from Driver after Evaluator is done."));
                    }
                }
                else if (_state != State.RUNNING)
                {
                    OnException(new InvalidOperationException(
                        string.Format(CultureInfo.InvariantCulture, "Evaluator received a control message but its state is not {0} but rather {1}", State.RUNNING, _state)));
                }
                else
                {
                    if (message.context_control != null)
                    {
                        Logger.Log(Level.Info, "Send task control message to ContextManager");
                        try
                        {
                            _contextManager.HandleTaskControl(message.context_control);
                            if (_contextManager.ContextStackIsEmpty() && _state == State.RUNNING)
                            {
                                Logger.Log(Level.Info, "Context stack is empty, done");
                                _state = State.DONE;
                                _heartBeatManager.OnNext(GetEvaluatorStatus());
                            }
                        }
                        catch (Exception e)
                        {
                            Utilities.Diagnostics.Exceptions.Caught(e, Level.Error, Logger);
                            OnException(e);
                            throw new InvalidOperationException(e.ToString(), e);
                        }
                    }
                    if (message.kill_evaluator != null)
                    {
                        Logger.Log(Level.Info, "Evaluator {0} has been killed by the driver.", _evaluatorId);
                        _state = State.KILLED;
                        _clock.Dispose();
                    }
                }
            }
        }

        public EvaluatorStatusProto GetEvaluatorStatus()
        {
            lock (_heartBeatManager)
            {
                Logger.Log(Level.Verbose, "Evaluator state: {0}", _state);
                return new EvaluatorStatusProto
                {
                    evaluator_id = _evaluatorId,
                    state = _state
                };
            }
        }

        public void OnNext(RuntimeStart runtimeStart)
        {
            lock (_heartBeatManager)
            {
                try
                {
                    Logger.Log(Level.Info, "Runtime start");
                    _pidStoreHelper.WritePID();
                    if (_state != State.INIT)
                    {
                        throw new InvalidOperationException("State should be init.");
                    }
                    _state = State.RUNNING;
                    _contextManager.Start();
                    _heartBeatManager.OnNext();
                }
                catch (ContextException e)
                {
                    Utilities.Diagnostics.Exceptions.Caught(e, Level.Error, Logger);
                    OnException(e.InnerException);
                }
                catch (Exception e)
                {
                    Utilities.Diagnostics.Exceptions.Caught(e, Level.Error, Logger);
                    OnException(e);
                }
            }
        }

        public void OnNext(RuntimeStop runtimeStop)
        {
            Logger.Log(Level.Verbose, "Runtime stop");

            lock (_heartBeatManager)
            {
                if (_state == State.RUNNING)
                {
                    const string msg = "RuntimeStopHandler invoked in state RUNNING.";
                    if (runtimeStop.Exception != null)
                    {
                        OnException(new ReefRuntimeException(msg, runtimeStop.Exception));
                    }
                    else
                    {
                        OnException(new ReefRuntimeException(msg));
                    }
                }
                else
                {
                    var successfulExit = true;
                    try
                    {
                        _contextManager.Dispose();
                        _evaluatorControlChannel.Dispose();
                    }
                    catch (Exception e)
                    {
                        successfulExit = false;
                        Utilities.Diagnostics.Exceptions.CaughtAndThrow(
                            new InvalidOperationException("Cannot stop evaluator properly", e),
                            Level.Error,
                            "Exception during shut down.",
                            Logger);
                    }
                    finally
                    {
                        _evaluatorExitLogger.LogExit(successfulExit);
                    }
                }
            }
        }

        public void OnNext(REEFMessage value)
        {
            if (value != null && value.evaluatorControl != null)
            {
                Logger.Log(Level.Verbose, "Received a REEFMessage with EvaluatorControl");
                Handle(value.evaluatorControl);
            }
        }

        internal void OnException(Exception e)
        {
            lock (_heartBeatManager)
            {
                Logger.Log(Level.Error, "Evaluator {0} failed with exception {1}.", _evaluatorId, e);
                _state = State.FAILED;

                byte[] errorBytes = null;

                try
                {
                    errorBytes = ByteUtilities.SerializeToBinaryFormat(e);
                }
                catch (SerializationException se)
                {
                    errorBytes = ByteUtilities.SerializeToBinaryFormat(
                        NonSerializableEvaluatorException.UnableToSerialize(e, se));
                }

                var evaluatorStatusProto = new EvaluatorStatusProto
                {
                    evaluator_id = _evaluatorId,
                    error = errorBytes,
                    state = _state
                };

                _heartBeatManager.OnNext(evaluatorStatusProto);
                _contextManager.Dispose();

                _evaluatorExitLogger.LogExit(false);
            }
        }

        public void OnError(Exception error)
        {
            throw new NotImplementedException();
        }

        public void OnCompleted()
        {
            throw new NotImplementedException();
        }
    }
}