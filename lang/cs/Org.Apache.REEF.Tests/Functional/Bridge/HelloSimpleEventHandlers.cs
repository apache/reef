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
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Net;
using System.Text;
using Org.Apache.REEF.Common.Tasks;
using Org.Apache.REEF.Driver;
using Org.Apache.REEF.Driver.Bridge;
using Org.Apache.REEF.Driver.Context;
using Org.Apache.REEF.Driver.Evaluator;
using Org.Apache.REEF.Driver.Task;
using Org.Apache.REEF.Examples.Tasks.HelloTask;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Tang.Util;
using Org.Apache.REEF.Utilities;
using Org.Apache.REEF.Utilities.Logging;
using IRunningTask = Org.Apache.REEF.Driver.Task.IRunningTask;

namespace Org.Apache.REEF.Tests.Functional.Bridge
{
    enum DriverStatus
    {
        Init = 0,
        Idle = 1,
        RunningTasks = 2,
        CompleteTasks = 3
    }

    internal enum TaskStatus
    {
        Submitting = 0,
        Running = 1,
        Completed = 2
    }

    /// <summary>
    /// A demo class that contains basic handlers. It runs given tasks and is able to get request from http server and start to run the tasks again. 
    /// It handle various http requests. It also monitoring task status and driver status.
    /// When IsRetain is configured as false. Context will be closed after executing all the tasks. 
    /// The default of IsRetain is true, that would retain the evaluators and wait for http requests to submit more jobs.
    /// </summary>
    public class HelloSimpleEventHandlers :
        IObserver<IDriverStarted>,
        IObserver<IAllocatedEvaluator>,
        IObserver<IActiveContext>,
        IObserver<ICompletedTask>,
        IObserver<IRunningTask>,
        IObserver<IFailedTask>,
        IObserver<IFailedEvaluator>,
        IObserver<ICompletedEvaluator>,
        IHttpHandler
    {
        private const int NumberOfTasks = 5;
        private static readonly Logger Logger = Logger.GetLogger(typeof(HelloSimpleEventHandlers));
        private readonly IList<IActiveContext> _activeContexts = new List<IActiveContext>();
        private DriverStatus _driveStatus;
        private TaskContext _taskContext;
        private readonly bool _isRetain;
        private readonly int _numberOfEvaluators;
        private readonly IEvaluatorRequestor _evaluatorRequestor;

        [Inject]
        private HelloSimpleEventHandlers(IEvaluatorRequestor evaluatorRequestor, 
            [Parameter(typeof(NumberOfEvaluators))] int numberOfEvaluators, 
            [Parameter(typeof(IsRetain))] bool isRetain)
        {
            Logger.Log(Level.Info, "HelloSimpleEventHandlers constructor");
            _taskContext = new TaskContext();
            _taskContext.TotalTasks = NumberOfTasks;
            _driveStatus = DriverStatus.Init;
            _isRetain = isRetain;
            _numberOfEvaluators = numberOfEvaluators;
            _evaluatorRequestor = evaluatorRequestor;
        }

        public static string ParsePathInfo(string pathInfo)
        {
            string[] p = pathInfo.Split('/');
            foreach (string s in p)
            {
                Logger.Log(Level.Info, s);
            }
            if (p.Length > 3)
            {
                return p[3];
            }
            return null;
        }

        public static void BuildHttpResponse(
            ReefHttpResponse response,
            HttpStatusCode httpStatusCode,
            string strResponse)
        {
            response.Status = httpStatusCode;
            response.OutputStream = ByteUtilities.StringToByteArrays(strResponse);
        }

        public static void BuildHttpResponse(
            ReefHttpResponse response,
            HttpStatusCode httpStatusCode,
            byte[] bytesResponse)
        {
            response.Status = httpStatusCode;
            response.OutputStream = bytesResponse;
        }

        public void OnNext(IDriverStarted driverStarted)
        {
            using (Logger.LogFunction("HelloSimpleEventHandlers::evaluatorRequestor received"))
            {
                int evaluatorsNumber = _numberOfEvaluators;
                int memory = 1024 * 3;
                int cpuCoreCount = 1;
                string rack = "WonderlandRack";
                string evaluatorBatchId = "evaluatorThatRequires3GBofMemory";
                var request =
                    _evaluatorRequestor.NewBuilder()
                        .SetNumber(evaluatorsNumber)
                        .SetMegabytes(memory)
                        .SetCores(cpuCoreCount)
                        .SetRackName(rack)
                        .SetEvaluatorBatchId(evaluatorBatchId)
                        .Build();
                _evaluatorRequestor.Submit(request);
            }
        }

        public void OnNext(IAllocatedEvaluator allocatedEvaluator)
        {
            string taskId = "Task_" + allocatedEvaluator.Id;
            var descriptor = allocatedEvaluator.GetEvaluatorDescriptor();
            Logger.Log(Level.Info, string.Format(CultureInfo.InvariantCulture, "Evaluator is assigned with {0} MB of memory and {1} cores.", descriptor.Memory, descriptor.VirtualCore));

            using (Logger.LogFunction("HelloSimpleEventHandlers::allocatedEvaluator received {0}.", taskId))
            {
                IConfiguration contextConfiguration = ContextConfiguration.ConfigurationModule.Set(ContextConfiguration.Identifier, "HelloSimpleEventHandlersContext_" + Guid.NewGuid().ToString("N")).Build();
                allocatedEvaluator.SubmitContext(contextConfiguration);
            }
        }

        public void OnNext(IActiveContext activeContext)
        {
            using (Logger.LogFunction("HelloSimpleEventHandlers::activeContext received"))
            {
                Logger.Log(Level.Info, string.Format(CultureInfo.InvariantCulture, "Received activeContext, EvaluatorId id: {0}", activeContext.EvaluatorId));
                _activeContexts.Add(activeContext);
                _driveStatus = DriverStatus.RunningTasks;
                SubmitNextTask(activeContext);
            }
        }

        public void OnNext(ICompletedTask value)
        {
            using (Logger.LogFunction("HelloSimpleEventHandlers::CompletedTask received"))
            {
                Logger.Log(Level.Info, "Received CompletedTask: {0}, task id: {1}.", value.Id, _taskContext.CurrentTaskId());
                
                var messageStr = value.Message == null || value.Message.Length == 0 ?
                    string.Empty : ByteUtilities.ByteArraysToString(value.Message);
                Logger.Log(Level.Verbose, "Message received from CompletedTask {0} is: [{1}]", value.Id, messageStr);

                _taskContext.UpdateTaskStatus(value.Id, TaskStatus.Completed);
                _taskContext.TaskCompleted++;
                SubmitNextTask(value.ActiveContext);
            }
        }

        public void OnError(Exception error)
        {
            Logger.Log(Level.Error, string.Format(CultureInfo.InvariantCulture, "Exception in coral handlers Msg: {1} Stack: {2}", error.Message, error.StackTrace));
        }

        public void OnCompleted()
        {
        }

        public void OnNext(IRunningTask value)
        {
           _taskContext.UpdateTaskStatus(_taskContext.CurrentTaskId(), TaskStatus.Running);
        }

        public void OnNext(IFailedTask value)
        {           
        }

        public void OnNext(IFailedEvaluator value)
        {            
        }

        public void OnNext(ICompletedEvaluator completedEvaluator)
        {
            string messageStr = string.Format(
                CultureInfo.InvariantCulture,
                "HelloSimpleEventHandlers: Evaluator [{0}] is done.",
                completedEvaluator.Id);
            Console.WriteLine(messageStr);
        }

        public string GetSpecification()
        {
            return "crystal";
        }

        public void OnHttpRequest(ReefHttpRequest request, ReefHttpResponse response)
        {
            string target = ParsePathInfo(request.PathInfo);
            Logger.Log(Level.Info, "Target: " + target + ". PathInfo: " + request.PathInfo);
            if (target != null && target.Equals("driverstatus"))
                {
                Logger.Log(Level.Info, "Target: " + target + ". Driver status: " + _driveStatus.ToString());
                string msg = string.Format(CultureInfo.CurrentCulture, "Current Driver status: {0} ", _driveStatus.ToString());
                BuildHttpResponse(response, HttpStatusCode.OK, msg);
                return;
            }

            if (target != null && target.Equals("taskstatus"))
            {
                Logger.Log(Level.Info, "Target: " + target + ". TaskStatus string: " + _taskContext.TaskStatusString());
                BuildHttpResponse(response, HttpStatusCode.OK, _taskContext.TaskStatusString());
                return;
            }

            if (target != null && target.ToLower(CultureInfo.CurrentCulture).Equals("run") && _driveStatus == DriverStatus.Init)
            {
                BuildHttpResponse(response, HttpStatusCode.OK, "Driver is not ready, wait a few second then send request again!!!");
                return;
            }

            if (target != null && target.ToLower(CultureInfo.CurrentCulture).Equals("run") && _driveStatus == DriverStatus.RunningTasks)
            {
                string msg = string.Format(CultureInfo.CurrentCulture,
                                           "A job is running. Please check driver status and then submit your job again.");
                BuildHttpResponse(response, HttpStatusCode.OK, msg);
                return;
            }

            if (target != null && target.ToLower(CultureInfo.CurrentCulture).Equals("run") && _driveStatus == DriverStatus.Idle)
            {
                string numberOfTasks = GetQueryValue(request.Querystring, "numberoftasks");
                if (numberOfTasks == null)
                {
                    BuildHttpResponse(response, HttpStatusCode.OK, "Please specify number of tasks to run");
                    return;
                }

                _driveStatus = DriverStatus.RunningTasks;
                using (Logger.LogFunction("HelloSimpleEventHandlers::Processing a new Job from web request"))
                {
                    _taskContext = new TaskContext();
                    _taskContext.TotalTasks = int.Parse(numberOfTasks, CultureInfo.CurrentCulture);
                    BuildHttpResponse(response, HttpStatusCode.OK, "Job from web request is submitted and is running!!!");
                }

                foreach (var c in _activeContexts)
                {
                    SubmitNextTask(c);
                }
                return;
            }
            BuildHttpResponse(response, HttpStatusCode.OK, "Unsupported query");
        }

        private static IDictionary<string, string> ParseQueryString(string queryString)
        {
            IDictionary<string, string> queryPairs = new Dictionary<string, string>();
            if (!string.IsNullOrEmpty(queryString))
            {
                string[] queries = queryString.Split('&');
                foreach (string query in queries)
                {
                    string[] pairs = query.Split('=');
                    if (pairs.Length == 2 && !pairs[0].Equals(string.Empty) && !pairs[1].Equals(string.Empty))
                    {
                        queryPairs[pairs[0]] = pairs[1];
                        Logger.Log(Level.Info, string.Format(CultureInfo.CurrentCulture, "query key: {0}, Query value: {1}.", pairs[0], pairs[1]));
                    }
                }
            }
            return queryPairs;
        }

        private static string GetQueryValue(string queryString, string name)
        {
            IDictionary<string, string> pairs = ParseQueryString(queryString);
            string v;
            pairs.TryGetValue(name, out v);
            return v;
        }

        private void SubmitNextTask(IActiveContext activeContext)
        {
            Logger.Log(Level.Info, "SubmitNextTask with evaluator id: " + activeContext.EvaluatorId);
            IConfiguration finalConfiguration = GetNextTaskConfiguration();
            if (null != finalConfiguration)
            {
                Logger.Log(Level.Info, "Executing task id " + _taskContext.CurrentTaskId());
                Logger.Log(Level.Info, string.Format(CultureInfo.InvariantCulture, "Submitting Task {0}", _taskContext.CurrentTaskId()));

                activeContext.SubmitTask(finalConfiguration);
            }
            else
            {
                if (_taskContext.TaskCompleted == _taskContext.TotalTasks)
                {
                    Logger.Log(Level.Info, "All tasks submitted and completed, active context remains idle");
                    _driveStatus = DriverStatus.Idle;
                    if (!_isRetain)
                    {
                        activeContext.Dispose();
                    }
                }
            }
        }

        private IConfiguration GetNextTaskConfiguration()
        {
            string nextTaskId = _taskContext.NextTaskId();
            Logger.Log(Level.Info, "GetNextTaskConfiguration, nextTaskId: " + nextTaskId);
            if (nextTaskId != null)
            {
                IConfiguration taskConfiguration = TaskConfiguration.ConfigurationModule
                    .Set(TaskConfiguration.Identifier, nextTaskId)
                    .Set(TaskConfiguration.Task, GenericType<HelloTask>.Class)
                    .Set(TaskConfiguration.OnMessage, GenericType<HelloTask.HelloDriverMessageHandler>.Class)
                    .Set(TaskConfiguration.OnSendMessage, GenericType<HelloTaskMessage>.Class)
                    .Build();
                return taskConfiguration;
            }
            return null;
        }
    }

    class TaskContext
    {
        private static readonly Logger Logger = Logger.GetLogger(typeof(TaskContext));
        private readonly IList<string> _taskIds = new List<string>();
        private readonly IDictionary<string, TaskStatus> _tasks = new Dictionary<string, TaskStatus>();

        public TaskContext()
        {
            NextTaskIndex = 0;
            TaskCompleted = 0;
        }

        public int TotalTasks { get; set; }

        public int NextTaskIndex { get; set; }

        public int TaskCompleted { get; set; }

        public string NextTaskId()
        {
           Logger.Log(Level.Verbose, "NextTaskId: " + NextTaskIndex);
           if (NextTaskIndex < TotalTasks)
           {
               string id = "Jan7DemoTask_" + DateTime.Now.Ticks;
               _taskIds.Add(id);
               _tasks.Add(id, TaskStatus.Submitting);
               NextTaskIndex++;
               return id;
           }
           return null;
        }

        public string CurrentTaskId()
        {
            Logger.Log(Level.Verbose, "CurrentTaskIndex: " + (NextTaskIndex - 1));
            if (NextTaskIndex <= TotalTasks)
            {
                Logger.Log(Level.Verbose, "CurrentTaskId: " + _taskIds[NextTaskIndex - 1]);
                return _taskIds[NextTaskIndex - 1];
            }
            return null; // either not started or completed
        }

        public void UpdateTaskStatus(string taskId, TaskStatus status)
        {
            _tasks[taskId] = status;
        }

        public string TaskStatusString()
        {
            Logger.Log(Level.Verbose, "TaskStatusString 1, nextTaskIndex: " + NextTaskIndex);
            StringBuilder sb = new StringBuilder();

            if (_tasks.Count > 0)
            {
                foreach (var pair in _tasks)
                {
                    sb.AppendLine("Task id: " + pair.Key + " Task status: " + pair.Value.ToString());
                }
            }
            else
            {
                sb.Append("No task is running yet");
            }

            return sb.ToString();
        }
    }

    [NamedParameter(Documentation = "NumberOfTasks", ShortName = "NumberOfTasks", DefaultValue = "2")]
    class NumberOfEvaluators : Name<Int32>
    {        
    }

    [NamedParameter(Documentation = "Retain", ShortName = "Retain", DefaultValue = "true")]
    class IsRetain : Name<bool>
    {
    }
}
