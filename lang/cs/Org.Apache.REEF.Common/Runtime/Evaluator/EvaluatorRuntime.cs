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

using System;
using System.Globalization;
using Org.Apache.REEF.Common.Protobuf.ReefProtocol;
using Org.Apache.REEF.Common.Runtime.Evaluator.Context;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Utilities;
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
        private readonly HeartBeatManager _heartBeatManager;
        private readonly IClock _clock;
        private readonly IDisposable _evaluatorControlChannel; 

        private State _state = State.INIT;

        [Inject]
        public EvaluatorRuntime(
            ContextManager contextManager,
            HeartBeatManager heartBeatManager)
        {
            using (Logger.LogFunction("EvaluatorRuntime::EvaluatorRuntime"))
            {
                _heartBeatManager = heartBeatManager;
                _clock = _heartBeatManager.EvaluatorSettings.RuntimeClock;
                _contextManager = contextManager;
                _evaluatorId = _heartBeatManager.EvaluatorSettings.EvalutorId;
                var remoteManager = _heartBeatManager.EvaluatorSettings.RemoteManager;

                ReefMessageProtoObserver driverObserver = new ReefMessageProtoObserver();

                // subscribe to driver proto message
                driverObserver.Subscribe(o => OnNext(o.Message));

                // register the driver observer
                _evaluatorControlChannel = remoteManager.RegisterObserver(driverObserver);

                // start the heart beat
                _clock.ScheduleAlarm(0, _heartBeatManager);
            }
        }

        public State State
        {
            get
            {
                return _state;
            }
        }

        public void Handle(EvaluatorControlProto message)
        {
            lock (_heartBeatManager)
            {
                Logger.Log(Level.Info, "Handle Evaluator control message");
                if (!message.identifier.Equals(_evaluatorId, StringComparison.OrdinalIgnoreCase))
                {
                    Handle(new InvalidOperationException(
                        string.Format(CultureInfo.InvariantCulture, "Identifier mismatch: message for evaluator id[{0}] sent to evaluator id[{1}]", message.identifier, _evaluatorId)));
                }
                else if (_state != State.RUNNING)
                {
                    Handle(new InvalidOperationException(
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
                                _clock.Dispose();
                            }
                        }
                        catch (Exception e)
                        {
                            Utilities.Diagnostics.Exceptions.Caught(e, Level.Error, Logger);
                            Handle(e);
                            Utilities.Diagnostics.Exceptions.Throw(new InvalidOperationException(e.ToString(), e), Logger);
                        }
                    }
                    if (message.kill_evaluator != null)
                    {
                        Logger.Log(Level.Info, string.Format(CultureInfo.InvariantCulture, "Evaluator {0} has been killed by the driver.", _evaluatorId));
                        _state = State.KILLED;
                        _clock.Dispose();
                    }
                }
            }
        }

        public EvaluatorStatusProto GetEvaluatorStatus()
        {
            Logger.Log(Level.Info, string.Format(CultureInfo.InvariantCulture, "Evaluator state : {0}", _state));
            EvaluatorStatusProto evaluatorStatusProto = new EvaluatorStatusProto
            {
                evaluator_id = _evaluatorId,
                state = _state
            };
            return evaluatorStatusProto;
        }

        public void OnNext(RuntimeStart runtimeStart)
        {
            lock (_evaluatorId)
            {
                try
                {
                    Logger.Log(Level.Info, "Runtime start");
                    if (_state != State.INIT)
                    {
                        var e = new InvalidOperationException("State should be init.");
                        Utilities.Diagnostics.Exceptions.Throw(e, Logger);
                    }
                    _state = State.RUNNING;
                    _contextManager.Start();
                    _heartBeatManager.OnNext();
                }
                catch (Exception e)
                {
                    Utilities.Diagnostics.Exceptions.Caught(e, Level.Error, Logger);
                    Handle(e);
                }
            }
        }

        public void OnNext(RuntimeStop runtimeStop)
        {
            Logger.Log(Level.Info, "Runtime stop");
            _contextManager.Dispose();

            if (_state == State.RUNNING)
            {
                _state = State.DONE;
                _heartBeatManager.OnNext();
            }
            try
            {
                _evaluatorControlChannel.Dispose();
            }
            catch (Exception e)
            {
                Utilities.Diagnostics.Exceptions.CaughtAndThrow(new InvalidOperationException("Cannot stop evaluator properly", e), Level.Error, "Exception during shut down.", Logger);
            }
            Logger.Log(Level.Info, "EvaluatorRuntime shutdown complete");        
        }

        public void OnNext(REEFMessage value)
        {
            if (value != null && value.evaluatorControl != null)
            {
                Logger.Log(Level.Info, "Received a REEFMessage with EvaluatorControl");
                Handle(value.evaluatorControl);
            }
        }

        private void Handle(Exception e)
        {
            lock (_heartBeatManager)
            {
                Logger.Log(Level.Error, string.Format(CultureInfo.InvariantCulture, "evaluator {0} failed with exception", _evaluatorId), e);
                _state = State.FAILED;
                string errorMessage = string.Format(
                        CultureInfo.InvariantCulture,
                        "failed with error [{0}] with message [{1}] and stack trace [{2}]",
                        e,
                        e.Message,
                        e.StackTrace);
                EvaluatorStatusProto evaluatorStatusProto = new EvaluatorStatusProto()
                {
                    evaluator_id = _evaluatorId,
                    error = ByteUtilities.StringToByteArrays(errorMessage),
                    state = _state
                };
                _heartBeatManager.OnNext(evaluatorStatusProto);
                _contextManager.Dispose();
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