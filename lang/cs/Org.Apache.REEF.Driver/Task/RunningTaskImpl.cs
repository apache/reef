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
using Org.Apache.REEF.Driver.Context;
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.Driver.Task
{
    [Obsolete("Driver core logic no longer needed in.NET")]
    public class RunningTaskImpl : IRunningTask
   {
       private static readonly Logger LOGGER = Logger.GetLogger(typeof(RunningTaskImpl));
       
       private readonly string _id;

       private readonly EvaluatorContext _evaluatorContext;

       public RunningTaskImpl(string taskId, EvaluatorContext evaluatorContext)
       {
           _id = taskId;
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
           ContextControlProto contextControlProto = new ContextControlProto();
           contextControlProto.stop_task = new StopTaskProto();
       }

       public void Dispose(byte[] message)
       {
           ContextControlProto contextControlProto = new ContextControlProto();
           contextControlProto.stop_task = new StopTaskProto();
           contextControlProto.task_message = message;
       }

       public void OnNext(byte[] message)
       {
           ContextControlProto contextControlProto = new ContextControlProto();
           contextControlProto.task_message = message;
       }

       public void Suspend(byte[] message)
       {
           ContextControlProto contextControlProto = new ContextControlProto();
           contextControlProto.suspend_task = new SuspendTaskProto();
           contextControlProto.task_message = message;
       }

       public void Suspend()
       {
           ContextControlProto contextControlProto = new ContextControlProto();
           contextControlProto.suspend_task = new SuspendTaskProto();
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
