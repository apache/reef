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

using Org.Apache.Reef.Common.Context;
using Org.Apache.Reef.Common.ProtoBuf.EvaluatorRunTimeProto;
using Org.Apache.Reef.Common.ProtoBuf.ReefProtocol;
using Org.Apache.Reef.Common.ProtoBuf.ReefServiceProto;
using Org.Apache.Reef.Evaluator;
using Org.Apache.Reef.Utilities;
using Org.Apache.Reef.Utilities.Logging;
using Org.Apache.Reef.Tang.Annotations;
using Org.Apache.Reef.Tang.Interface;
using Org.Apache.Reef.Wake.Remote;
using Org.Apache.Reef.Wake.Time;
using Org.Apache.Reef.Wake.Time.Runtime.Event;
using System;
using System.Globalization;

namespace Org.Apache.Reef.Common
{
    public class EvaluatorRuntime : IObserver<RuntimeStart>, IObserver<RuntimeStop>, IObserver<REEFMessage>
    {
        private static readonly Logger LOGGER = Logger.GetLogger(typeof(EvaluatorRuntime));
        
        private readonly string _evaluatorId;

        private readonly ContextManager _contextManager;

        private readonly HeartBeatManager _heartBeatManager;

        private readonly IRemoteManager<REEFMessage> _remoteManager;

        private readonly IClock _clock;

        private State _state = State.INIT;

        private IDisposable _evaluatorControlChannel; 

        [Inject]
        public EvaluatorRuntime(
            ContextManager contextManager,
            HeartBeatManager heartBeatManager)
        {
            using (LOGGER.LogFunction("EvaluatorRuntime::EvaluatorRuntime"))
            {
                _clock = heartBeatManager.EvaluatorSettings.RuntimeClock;
                _heartBeatManager = heartBeatManager;
                _contextManager = contextManager;
                _evaluatorId = heartBeatManager.EvaluatorSettings.EvalutorId;
                _remoteManager = heartBeatManager.EvaluatorSettings.RemoteManager;

                ReefMessageProtoObserver driverObserver = new ReefMessageProtoObserver();

                // subscribe to driver proto message
                driverObserver.Subscribe(o => OnNext(o.Message));

                // register the driver observer
                _evaluatorControlChannel = _remoteManager.RegisterObserver(driverObserver);

                // start the hearbeat
                _clock.ScheduleAlarm(0, heartBeatManager);
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
                LOGGER.Log(Level.Info, "Handle Evaluator control message");
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
                        LOGGER.Log(Level.Info, "Send task control message to ContextManager");
                        try
                        {
                            _contextManager.HandleTaskControl(message.context_control);
                            if (_contextManager.ContextStackIsEmpty() && _state == State.RUNNING)
                            {
                                LOGGER.Log(Level.Info, "Context stack is empty, done");
                                _state = State.DONE;
                                _heartBeatManager.OnNext(GetEvaluatorStatus());
                                _clock.Dispose();
                            }
                        }
                        catch (Exception e)
                        {
                            Org.Apache.Reef.Utilities.Diagnostics.Exceptions.Caught(e, Level.Error, LOGGER);
                            Handle(e);
                            Org.Apache.Reef.Utilities.Diagnostics.Exceptions.Throw(new InvalidOperationException(e.ToString(), e), LOGGER);
                        }
                    }
                    if (message.kill_evaluator != null)
                    {
                        LOGGER.Log(Level.Info, string.Format(CultureInfo.InvariantCulture, "Evaluator {0} has been killed by the driver.", _evaluatorId));
                        _state = State.KILLED;
                        _clock.Dispose();
                    }
                }
            }
        }

        public EvaluatorStatusProto GetEvaluatorStatus()
        {
            LOGGER.Log(Level.Info, string.Format(CultureInfo.InvariantCulture, "Evaluator state : {0}", _state));
            EvaluatorStatusProto evaluatorStatusProto = new EvaluatorStatusProto()
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
                    LOGGER.Log(Level.Info, "Runtime start");
                    if (_state != State.INIT)
                    {
                        var e = new InvalidOperationException("State should be init.");
                        Org.Apache.Reef.Utilities.Diagnostics.Exceptions.Throw(e, LOGGER);
                    }
                    _state = State.RUNNING;
                    _contextManager.Start();
                    _heartBeatManager.OnNext();
                }
                catch (Exception e)
                {
                    Org.Apache.Reef.Utilities.Diagnostics.Exceptions.Caught(e, Level.Error, LOGGER);
                    Handle(e);
                }
            }
        }

        void IObserver<RuntimeStart>.OnError(Exception error)
        {
            throw new NotImplementedException();
        }

        void IObserver<REEFMessage>.OnCompleted()
        {
            throw new NotImplementedException();
        }

        void IObserver<REEFMessage>.OnError(Exception error)
        {
            throw new NotImplementedException();
        }

        void IObserver<RuntimeStop>.OnCompleted()
        {
            throw new NotImplementedException();
        }

        void IObserver<RuntimeStop>.OnError(Exception error)
        {
            throw new NotImplementedException();
        }

        void IObserver<RuntimeStart>.OnCompleted()
        {
            throw new NotImplementedException();
        }

        public void OnNext(RuntimeStop runtimeStop)
        {
            LOGGER.Log(Level.Info, "Runtime stop");
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
                Org.Apache.Reef.Utilities.Diagnostics.Exceptions.CaughtAndThrow(new InvalidOperationException("Cannot stop evaluator properly", e), Level.Error, "Exception during shut down.", LOGGER);
            }
            LOGGER.Log(Level.Info, "EvaluatorRuntime shutdown complete");        
        }

        public void OnNext(REEFMessage value)
        {
            if (value != null && value.evaluatorControl != null)
            {
                LOGGER.Log(Level.Info, "Received a REEFMessage with EvaluatorControl");
                Handle(value.evaluatorControl);
            }
        }

        private void Handle(Exception e)
        {
            lock (_heartBeatManager)
            {
                LOGGER.Log(Level.Error, string.Format(CultureInfo.InvariantCulture, "evaluator {0} failed with exception", _evaluatorId), e);
                _state = State.FAILED;
                string errorMessage = string.Format(
                        CultureInfo.InvariantCulture,
                        "failed with error [{0}] with mesage [{1}] and stack trace [{2}]",
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
    }
}