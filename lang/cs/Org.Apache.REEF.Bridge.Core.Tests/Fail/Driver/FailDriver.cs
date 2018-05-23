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
using System.Diagnostics;
using System.Linq;
using Org.Apache.REEF.Bridge.Core.Tests.Fail.ThreadInterruptedException;
using Org.Apache.REEF.Common.Context;
using Org.Apache.REEF.Common.Tasks;
using Org.Apache.REEF.Driver;
using Org.Apache.REEF.Driver.Context;
using Org.Apache.REEF.Driver.Evaluator;
using Org.Apache.REEF.Driver.Task;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Exceptions;
using Org.Apache.REEF.Tang.Util;
using Org.Apache.REEF.Utilities;
using Org.Apache.REEF.Utilities.Logging;
using Org.Apache.REEF.Wake.Time;
using Org.Apache.REEF.Wake.Time.Event;

namespace Org.Apache.REEF.Bridge.Core.Tests.Fail.Driver
{
    internal sealed class FailDriver : 
        IObserver<IAllocatedEvaluator>,
        IObserver<ICompletedEvaluator>,
        IObserver<IFailedEvaluator>,
        IObserver<IRunningTask>, 
        IObserver<ICompletedTask>, 
        IObserver<IFailedTask>, 
        IObserver<ITaskMessage>, 
        IObserver<ISuspendedTask>, 
        IObserver<IFailedContext>, 
        IObserver<IClosedContext>, 
        IObserver<IContextMessage>, 
        IObserver<IActiveContext>,
        IObserver<IDriverStarted>,
        IObserver<IDriverStopped>,
        IObserver<Alarm>
    {
        private static readonly Logger Log = Logger.GetLogger(typeof(FailDriver));

        [NamedParameter("fail message type name")]
        internal sealed class FailMsgTypeName : Name<string>
        {
        }

        private enum DriverState
        {
            Init, SendMsg, Suspend, Resume, Close, Failed
        }


        private static readonly byte[] HelloStringByteArray = ByteUtilities.StringToByteArrays("MESSAGE::HELLO");

        private static readonly int MsgDelay = 1000;

        private static readonly ExpectedMessage[] EventSequence = new ExpectedMessage[]
        {
            new ExpectedMessage(typeof(FailDriver), ExpectedMessage.RequiredFlag.Required),
            new ExpectedMessage(typeof(IDriverStarted), ExpectedMessage.RequiredFlag.Required),
            new ExpectedMessage(typeof(IAllocatedEvaluator), ExpectedMessage.RequiredFlag.Required),
            new ExpectedMessage(typeof(IFailedEvaluator), ExpectedMessage.RequiredFlag.Optional),
            new ExpectedMessage(typeof(IActiveContext), ExpectedMessage.RequiredFlag.Required),
            new ExpectedMessage(typeof(IContextMessage), ExpectedMessage.RequiredFlag.Optional),
            new ExpectedMessage(typeof(IFailedContext), ExpectedMessage.RequiredFlag.Optional),
            new ExpectedMessage(typeof(IRunningTask), ExpectedMessage.RequiredFlag.Required),
            new ExpectedMessage(typeof(Alarm), ExpectedMessage.RequiredFlag.Required),
            new ExpectedMessage(typeof(ITaskMessage), ExpectedMessage.RequiredFlag.Required),
            new ExpectedMessage(typeof(Alarm), ExpectedMessage.RequiredFlag.Required),
            new ExpectedMessage(typeof(ISuspendedTask), ExpectedMessage.RequiredFlag.Required),
            new ExpectedMessage(typeof(IRunningTask), ExpectedMessage.RequiredFlag.Required),
            new ExpectedMessage(typeof(Alarm), ExpectedMessage.RequiredFlag.Required),
            new ExpectedMessage(typeof(IFailedTask), ExpectedMessage.RequiredFlag.Optional),
            new ExpectedMessage(typeof(ICompletedTask), ExpectedMessage.RequiredFlag.Required),
            new ExpectedMessage(typeof(ICompletedTask), ExpectedMessage.RequiredFlag.Optional),
            new ExpectedMessage(typeof(ICompletedEvaluator), ExpectedMessage.RequiredFlag.Required),
            new ExpectedMessage(typeof(IDriverStopped), ExpectedMessage.RequiredFlag.Required)
        };

        private static readonly Dictionary<string, Type> TypeHash = new Dictionary<string, Type>()
        {
            {typeof(NoopTask).FullName, typeof(NoopTask)},
            {typeof(FailDriver).FullName, typeof(FailDriver)},
            {typeof(IDriverStarted).FullName, typeof(IDriverStarted)},
            {typeof(IAllocatedEvaluator).FullName, typeof(IAllocatedEvaluator)},
            {typeof(IFailedEvaluator).FullName, typeof(IFailedEvaluator)},
            {typeof(IActiveContext).FullName, typeof(IActiveContext)},
            {typeof(IContextMessage).FullName, typeof(IContextMessage)},
            {typeof(IFailedContext).FullName, typeof(IFailedContext)},
            {typeof(IRunningTask).FullName, typeof(IRunningTask)},
            {typeof(Alarm).FullName, typeof(Alarm)},
            {typeof(ISuspendedTask).FullName, typeof(ISuspendedTask)},
            {typeof(IFailedTask).FullName, typeof(IFailedTask)},
            {typeof(ICompletedTask).FullName, typeof(ICompletedTask)},
            {typeof(ITaskMessage).FullName, typeof(ITaskMessage)},
            {typeof(ICompletedEvaluator).FullName, typeof(ICompletedEvaluator)},
            {typeof(IDriverStopped).FullName, typeof(IDriverStopped)}
        };

        private readonly Type _failMsgClass;
        private readonly IEvaluatorRequestor _requestor;
        private readonly IClock _clock;
        private IRunningTask _task = null;
        private int _expectIdx = 0;
        private DriverState _state = DriverState.Init;

        [Inject]
        public FailDriver([Parameter(Value = typeof(FailMsgTypeName))] string failMsgTypeName,
            IEvaluatorRequestor requestor, IClock clock)
        {
            _clock = clock;
            _requestor = requestor;
            if (TypeHash.ContainsKey(failMsgTypeName))
            {
                _failMsgClass = TypeHash[failMsgTypeName];
            }
            else
            {
                _failMsgClass = typeof(FailDriver).Assembly.GetType(failMsgTypeName, true);
            }
            CheckMsgOrder(this);
        }

        public void OnError(System.Exception error)
        {
            throw new NotImplementedException();
        }

        public void OnCompleted()
        {
            throw new NotImplementedException();
        }

        private void CheckMsgOrder(object obj)
        {
            string msgClassName = _failMsgClass.FullName;
            Log.Log(Level.Info, "Driver state {0} event sequence {1} message obj type {2}", 
                new object[] { _state, EventSequence[_expectIdx].Type, obj.GetType()});

            if (_state == DriverState.Failed)
            {
                // If already failed, do not do anything
                return;
            }

            // Simulate failure at this step?
            if (_failMsgClass.IsInstanceOfType(obj))
            {
                _state = DriverState.Failed;
            }

            // Make sure events arrive in the right order (specified in EVENT_SEQUENCE):
            bool notFound = true;
            for (; _expectIdx < EventSequence.Length; ++_expectIdx)
            {
                if (EventSequence[_expectIdx].Type.IsInstanceOfType(obj))
                {
                    Log.Log(Level.Info, "Object type {0} is instance of expected type {1}", new object[] {obj.GetType(), EventSequence[_expectIdx].Type});
                    notFound = false;
                    break;
                }
                else if (EventSequence[_expectIdx].Flag == ExpectedMessage.RequiredFlag.Required)
                {
                    Log.Log(Level.Info, "Object type {0} is NOT instance of expected type {1}", new object[] {obj.GetType(), EventSequence[_expectIdx].Type});
                    break;
                }
            }

            if (notFound)
            {
                Log.Log(Level.Info, "Event out of sequence: Driver state {0} event sequence {1} message obj type {2}", 
                    new object[] { _state, EventSequence[_expectIdx].Type, obj.GetType()});
                throw new DriverSideFailure("Event out of sequence: " + msgClassName);
            }

            Log.Log(Level.Info, "{0}: send: {1} got: {2}", new object[] {
                _state, EventSequence[_expectIdx], msgClassName});

            ++_expectIdx;

            if (_state == DriverState.Failed)
            {
                SimulatedDriverFailure ex = new SimulatedDriverFailure(
                    "Simulated Failure at FailDriver :: " + msgClassName);
                Log.Log(Level.Info, "Simulated Failure:", ex);
                throw ex;
            }
        }


        private sealed class ExpectedMessage
        {
            public enum RequiredFlag
            {
                Required,
                Optional
            }

            public RequiredFlag Flag { get; }

            public Type Type { get; }

            public ExpectedMessage(Type type, RequiredFlag flag)
            {
                Type = type;
                Flag = flag;
            }
        }

        public void OnNext(IAllocatedEvaluator eval)
        {
            CheckMsgOrder(eval);
            try
            {
                eval.SubmitContext(ContextConfiguration.ConfigurationModule
                    .Set(ContextConfiguration.Identifier, "FailContext_" + eval.Id)
                    .Build());
            }
            catch (BindException ex)
            {
                Log.Log(Level.Warning, "Context configuration error", ex);
                throw new IllegalStateException("context configuration error", ex);
            }
        }

        public void OnNext(ICompletedEvaluator eval)
        {
            CheckMsgOrder(eval);
            // noop
        }

        public void OnNext(IFailedEvaluator eval)
        {
            Log.Log(Level.Warning, "Evaluator failed: " + eval.Id, eval.EvaluatorException);
            CheckMsgOrder(eval);
            throw new IllegalStateException("failed evaluator illegal state", eval.EvaluatorException);
        }

        public void OnNext(IActiveContext context)
        {
            CheckMsgOrder(context);
            try
            {
                context.SubmitTask(TaskConfiguration.ConfigurationModule
                    .Set(TaskConfiguration.Identifier, "FailTask_" + context.Id)
                    .Set(TaskConfiguration.Task, GenericType<NoopTask>.Class)
                    .Set(TaskConfiguration.OnMessage, GenericType<NoopTask>.Class)
                    .Set(TaskConfiguration.OnSuspend, GenericType<NoopTask>.Class)
                    .Set(TaskConfiguration.OnClose, GenericType<NoopTask>.Class)
                    .Set(TaskConfiguration.OnTaskStop, GenericType<NoopTask>.Class)
                    .Set(TaskConfiguration.OnSendMessage, GenericType<NoopTask>.Class)
                    .Build());
            }
            catch (BindException ex)
            {
                Log.Log(Level.Warning, "Task configuration error", ex);
                throw new IllegalStateException("task configuration error", ex);
            }
        }


        public void OnNext(IContextMessage message)
        {
            CheckMsgOrder(message);
            // noop
        }


        public void OnNext(IClosedContext context)
        {
            CheckMsgOrder(context);
            // noop
        }


        public void OnNext(IFailedContext context)
        {
            Log.Log(Level.Warning, "Context failed: " + context.Id);
            CheckMsgOrder(context);
        }


        public void OnNext(IRunningTask runningTask)
        {
            CheckMsgOrder(runningTask);
            _task = runningTask;
            switch (_state)
            {
                case DriverState.Init:
                    Log.Log(Level.Info, "Move to state {0}", DriverState.SendMsg);
                    _state = DriverState.SendMsg;
                    break;
                case DriverState.Resume:
                    Log.Log(Level.Info, "Move to state {0}", DriverState.Close);
                    _state = DriverState.Close;
                    break;
                default:
                    Log.Log(Level.Warning, "Unexpected state at TaskRuntime: {0}", _state);
                    throw new DriverSideFailure("Unexpected state: " + _state);
            }

            // After a delay, send message or suspend the task:
            _clock.ScheduleAlarm(MsgDelay, this);
        }


        public void OnNext(ISuspendedTask suspendedTask)
        {
            CheckMsgOrder(suspendedTask);
            _state = DriverState.Resume;
            try
            {
                suspendedTask.ActiveContext.SubmitTask(TaskConfiguration.ConfigurationModule
                    .Set(TaskConfiguration.Identifier, suspendedTask.Id + "_RESUMED")
                    .Set(TaskConfiguration.Task, GenericType<NoopTask>.Class)
                    .Set(TaskConfiguration.OnMessage, GenericType<NoopTask>.Class)
                    .Set(TaskConfiguration.OnSuspend, GenericType<NoopTask>.Class)
                    .Set(TaskConfiguration.OnClose, GenericType<NoopTask>.Class)
                    .Set(TaskConfiguration.OnTaskStop, GenericType<NoopTask>.Class)
                    .Set(TaskConfiguration.OnSendMessage, GenericType<NoopTask>.Class)
                    .Set(TaskConfiguration.Memento, ByteUtilities.ByteArraysToString(HelloStringByteArray))
                    .Build());
            }
            catch (BindException ex)
            {
                Log.Log(Level.Error, "Task configuration error", ex);
                throw new DriverSideFailure("Task configuration error", ex);
            }
        }

        public void OnNext(ITaskMessage msg)
        {
            CheckMsgOrder(msg);
            Debug.Assert(Enumerable.SequenceEqual(HelloStringByteArray, msg.Message));
            Debug.Assert(_state == DriverState.SendMsg);
            _state = DriverState.Suspend;
            _clock.ScheduleAlarm(MsgDelay, this);
        }

        public void OnNext(IFailedTask failedTask)
        {
            Log.Log(Level.Warning, "Task failed: " + failedTask.Id, failedTask.Reason.OrElse(null));
            CheckMsgOrder(failedTask);
            if (failedTask.GetActiveContext().IsPresent())
            {
                failedTask.GetActiveContext().Value.Dispose();
            }
        }

        public void OnNext(ICompletedTask completedTask)
        {
            CheckMsgOrder(completedTask);
            completedTask.ActiveContext.Dispose();
        }

        public void OnNext(IDriverStarted time)
        {
            CheckMsgOrder(time);
            _requestor.Submit(_requestor.NewBuilder()
                .SetNumber(1).SetMegabytes(128).SetCores(1).Build());
        }

        public void OnNext(Alarm time)
        {
            CheckMsgOrder(time);
            switch (_state)
            {
                case DriverState.SendMsg:
                    Log.Log(Level.Info, "Send message to task {0}", _task.Id);
                    _task.Send(HelloStringByteArray);
                    break;
                case DriverState.Suspend:
                    Log.Log(Level.Info, "Suspend task {0}", _task.Id);
                    _task.Suspend();
                    break;
                case DriverState.Close:
                    Log.Log(Level.Info, "Close task {0}", _task.Id);
                    _task.Dispose();
                    break;
                default:
                    Log.Log(Level.Warning, "Unexpected state at AlarmHandler: {0}", _state);
                    throw new DriverSideFailure("Unexpected state: " + _state);
            }
        }

        public void OnNext(IDriverStopped time)
        {
            CheckMsgOrder(time);
            // noop
        }
    }
}
