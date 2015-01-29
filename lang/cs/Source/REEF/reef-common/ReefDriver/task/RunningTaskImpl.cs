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
using Org.Apache.Reef.Common.ProtoBuf.EvaluatorRunTimeProto;
using Org.Apache.Reef.Driver.Context;
using Org.Apache.Reef.Utilities.Logging;
using System.Globalization;

namespace Org.Apache.Reef.Driver.Task
{
   public class RunningTaskImpl : IRunningTask
   {
       private static readonly Logger LOGGER = Logger.GetLogger(typeof(RunningTaskImpl));
       
       private string _id;

       private EvaluatorManager _evaluatorManager;

       private EvaluatorContext _evaluatorContext;

       public RunningTaskImpl(EvaluatorManager evaluatorManager, string taskId, EvaluatorContext evaluatorContext)
       {
           LOGGER.Log(Level.Info, string.Format(CultureInfo.InvariantCulture, "INIT: TaskRuntime id [{0}] on evaluator id [{1}]", taskId, evaluatorManager.Id));
           _id = taskId;
           _evaluatorManager = evaluatorManager;
           _evaluatorContext = evaluatorContext;
       }

       public string Id
       {
           get
           {
               return _id;
           }

           set
           {
           }
       }

       public IActiveContext ActiveContext
       {
           get
           {
               return _evaluatorContext;
           }

           set
           {
           }
       }

       public void Dispose()
       {
           LOGGER.Log(Level.Info, string.Format(CultureInfo.InvariantCulture, "DISPOSE: TaskRuntime id [{0}] on evaluator id [{1}]", _id, _evaluatorManager.Id));
           ContextControlProto contextControlProto = new ContextControlProto();
           contextControlProto.stop_task = new StopTaskProto();
           _evaluatorManager.Handle(contextControlProto);
       }

       public void Dispose(byte[] message)
       {
           LOGGER.Log(Level.Info, string.Format(CultureInfo.InvariantCulture, "DISPOSE: TaskRuntime id [{0}] on evaluator id [{1}] with message", _id, _evaluatorManager.Id));
           ContextControlProto contextControlProto = new ContextControlProto();
           contextControlProto.stop_task = new StopTaskProto();
           contextControlProto.task_message = message;
           _evaluatorManager.Handle(contextControlProto);
       }

       public void OnNext(byte[] message)
       {
           LOGGER.Log(Level.Info, string.Format(CultureInfo.InvariantCulture, "MESSAGE: TaskRuntime id [{0}] on evaluator id [{1}]", _id, _evaluatorManager.Id));
           ContextControlProto contextControlProto = new ContextControlProto();
           contextControlProto.task_message = message;
           _evaluatorManager.Handle(contextControlProto);
       }

       public void Suspend(byte[] message)
       {
           LOGGER.Log(Level.Info, string.Format(CultureInfo.InvariantCulture, "SUSPEND: TaskRuntime id [{0}] on evaluator id [{1}] with message", _id, _evaluatorManager.Id));
           ContextControlProto contextControlProto = new ContextControlProto();
           contextControlProto.suspend_task = new SuspendTaskProto();
           contextControlProto.task_message = message;
           _evaluatorManager.Handle(contextControlProto);
       }

       public void Suspend()
       {
           LOGGER.Log(Level.Info, string.Format(CultureInfo.InvariantCulture, "SUSPEND: TaskRuntime id [{0}] on evaluator id [{1}]", _id, _evaluatorManager.Id));
           ContextControlProto contextControlProto = new ContextControlProto();
           contextControlProto.suspend_task = new SuspendTaskProto();
           _evaluatorManager.Handle(contextControlProto);
       }

       public override string ToString()
       {
           return "TaskRuntime with taskId = " + _id;
       }

       public override int GetHashCode()
       {
           return _id.GetHashCode();
       }

       public void Send(byte[] message)
       {
           LOGGER.Log(Level.Info, "RunningTaskImpl.Send() is called");
       }
    }
}
