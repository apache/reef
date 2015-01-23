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
using System.Threading.Tasks;
using Org.Apache.Reef.Common.io;
using Org.Apache.Reef.Driver;
using Org.Apache.Reef.Driver.Bridge;
using Org.Apache.Reef.Driver.Context;
using Org.Apache.Reef.Driver.Evaluator;
using Org.Apache.Reef.Driver.Task;
using Org.Apache.Reef.IO.Network.Naming;
using Org.Apache.Reef.Tasks;
using Org.Apache.Reef.Utilities;
using Org.Apache.Reef.Utilities.Logging;
using Org.Apache.Reef.Tang.Annotations;
using Org.Apache.Reef.Tang.Interface;
using Org.Apache.Reef.Tang.Util;

namespace Org.Apache.Reef.Examples.HelloCLRBridge.handlers
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
    /// A demo class that contains basic handlers. It runs given tasks and is able to get request from http server and start to ren the tasks again. 
    /// It handle various http requests. It also monitoring task status and driver status.
    /// </summary>
    public class HelloSimpleEventHandlers :
        IObserver<IEvaluatorRequestor>,
        IObserver<IAllocatedEvaluator>,
        IObserver<IActiveContext>,
        IObserver<ICompletedTask>,
        IObserver<IRunningTask>,
        IObserver<IFailedTask>,
        IObserver<IFailedEvaluator>,
        IObserver<ICompletedEvaluator>,
        IStartHandler,
        IHttpHandler
    {
        private const int NumberOfTasks = 5;
        private static readonly Logger LOGGER = Logger.GetLogger(typeof(HelloSimpleEventHandlers));
        private IAllocatedEvaluator _allocatedEvaluator;
        private IActiveContext _activeContext;
        private IList<IActiveContext> _activeContexts = new List<IActiveContext>();
        private DriverStatus driveStatus;
        private TaskContext _taskContext;

        [Inject]
        public HelloSimpleEventHandlers()
        {
            LOGGER.Log(Level.Info, "HelloSimpleEventHandlers constructor");
            CreateClassHierarchy();
            Identifier = "HelloSimpleEventHandlers";
            _taskContext = new TaskContext();
            _taskContext.TotalTasks = NumberOfTasks;
            driveStatus = DriverStatus.Init;
        }

        public string Identifier { get; set; }

        public static string ParsePathInfo(string pathInfo)
        {
            string[] p = pathInfo.Split('/');
            foreach (string s in p)
            {
                LOGGER.Log(Level.Info, s);
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

        public void OnNext(IEvaluatorRequestor evalutorRequestor)
        {
            using (LOGGER.LogFunction("HelloSimpleEventHandlers::evalutorRequestor received"))
            {
                int evaluatorsNumber = 2;
                int memory = 1024 * 3;
                int cpuCoreCount = 1;
                string rack = "WonderlandRack";
                string evaluatorBatchId = "evaluatorThatRequires3GBofMemory";
                EvaluatorRequest request = new EvaluatorRequest(evaluatorsNumber, memory, cpuCoreCount, rack, evaluatorBatchId);

                evalutorRequestor.Submit(request);
            }
        }

        public void OnNext(IAllocatedEvaluator allocatedEvaluator)
        {
            string taskId = "Task_" + allocatedEvaluator.Id;
            using (LOGGER.LogFunction("HelloSimpleEventHandlers::allocatedEvaluator received {0}.", taskId))
            {
                _allocatedEvaluator = allocatedEvaluator;

                IConfiguration contextConfiguration = ContextConfiguration.ConfigurationModule.Set(ContextConfiguration.Identifier, "HelloSimpleEventHandlersContext_" + Guid.NewGuid().ToString("N")).Build();

                allocatedEvaluator.SubmitContext(contextConfiguration);
            }
        }

        public void OnNext(IActiveContext activeContext)
        {
            using (LOGGER.LogFunction("HelloSimpleEventHandlers::activeContext received"))
            {
                LOGGER.Log(Level.Info, string.Format(CultureInfo.InvariantCulture, "Received activeContext, EvaluatorId id: {0}", activeContext.EvaluatorId));
                _activeContext = activeContext;
                _activeContexts.Add(activeContext);
                driveStatus = DriverStatus.RunningTasks;
                SubmitNextTask(activeContext);
            }
        }

        public void OnNext(ICompletedTask value)
        {
            using (LOGGER.LogFunction("HelloSimpleEventHandlers::CompletedTask received"))
            {
                LOGGER.Log(Level.Info, string.Format(CultureInfo.InvariantCulture, "Received CompletedTask: {0}, task id: {1}", value.Id, _taskContext.CurrentTaskId()));
                _activeContext = value.ActiveContext;
                _taskContext.UpdateTaskStatus(value.Id, TaskStatus.Completed);
                _taskContext.TaskCompleted++;
                SubmitNextTask(value.ActiveContext);
            }
        }

        public void OnError(Exception error)
        {
            LOGGER.Log(Level.Error, string.Format(CultureInfo.InvariantCulture, "Exception in coral handlers Msg: {1} Stack: {2}", error.Message, error.StackTrace));
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
            LOGGER.Log(Level.Info, "Target: " + target + ". PathInfo: " + request.PathInfo);
            //if (target != null && target.ToLower(CultureInfo.CurrentCulture).Equals("driverstatus"))
            if (target != null && target.Equals("driverstatus"))
                {
                LOGGER.Log(Level.Info, "Target: " + target + ". Driver status: " + driveStatus.ToString());
                string msg = string.Format(CultureInfo.CurrentCulture, "Current Driver status: {0} ", driveStatus.ToString());
                BuildHttpResponse(response, HttpStatusCode.OK, msg);
                return;
            }

            if (target != null && target.Equals("taskstatus"))
            {
                LOGGER.Log(Level.Info, "Target: " + target + ". TaskStatus string: " + _taskContext.TaskStatusString());
                BuildHttpResponse(response, HttpStatusCode.OK, _taskContext.TaskStatusString());
                return;
            }

            if (target != null && target.ToLower(CultureInfo.CurrentCulture).Equals("run") && driveStatus == DriverStatus.Init)
            {
                BuildHttpResponse(response, HttpStatusCode.OK, "Driver is not ready, wait a few second then send request again!!!");
                return;
            }

            if (target != null && target.ToLower(CultureInfo.CurrentCulture).Equals("run") && driveStatus == DriverStatus.RunningTasks)
            {
                string msg = string.Format(CultureInfo.CurrentCulture,
                                           "A job is running. Please check driver status and then submit your job again.");
                BuildHttpResponse(response, HttpStatusCode.OK, msg);
                return;
            }

            if (target != null && target.ToLower(CultureInfo.CurrentCulture).Equals("run") && driveStatus == DriverStatus.Idle)
            {
                string numberOfTasks = getQueryValue(request.Querystring, "numberoftasks");
                if (numberOfTasks == null)
                {
                    BuildHttpResponse(response, HttpStatusCode.OK, "Please specify number of tasks to run");
                    return;
                }

                driveStatus = DriverStatus.RunningTasks;
                using (LOGGER.LogFunction("HelloSimpleEventHandlers::Processing a new Job from web request"))
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
            if (queryString != null && queryString.Length > 0)
            {
                string[] queries = queryString.Split('&');
                foreach (string query in queries)
                {
                    string[] pairs = query.Split('=');
                    if (pairs.Length == 2 && !pairs[0].Equals(string.Empty) && !pairs[1].Equals(string.Empty))
                    {
                        queryPairs[pairs[0]] = pairs[1];
                        LOGGER.Log(Level.Info, string.Format(CultureInfo.CurrentCulture, "query key: {0}, Query value: {1}.", pairs[0], pairs[1]));
                    }
                }
            }
            return queryPairs;
        }

        private static string getQueryValue(string queryString, string name)
        {
            IDictionary<string, string> pairs = ParseQueryString(queryString);
            string v;
            pairs.TryGetValue(name, out v);
            return v;
        }

        private void CreateClassHierarchy()
        {
            HashSet<string> clrDlls = new HashSet<string>();
            clrDlls.Add(typeof(IDriver).Assembly.GetName().Name);
            clrDlls.Add(typeof(ITask).Assembly.GetName().Name);
            clrDlls.Add(typeof(HelloTask).Assembly.GetName().Name);
            clrDlls.Add(typeof(INameClient).Assembly.GetName().Name);
            clrDlls.Add(typeof(NameClient).Assembly.GetName().Name);

            ClrHandlerHelper.GenerateClassHierarchy(clrDlls);
        }

        private void SubmitNextTask(IActiveContext activeContext)
        {
            LOGGER.Log(Level.Info, "SubmitNextTask with evaluatorid: " + activeContext.EvaluatorId);
            IConfiguration finalConfiguration = GetNextTaskConfiguration();
            if (null != finalConfiguration)
            {
                LOGGER.Log(Level.Info, "Executing task id " + _taskContext.CurrentTaskId());
                LOGGER.Log(Level.Info, string.Format(CultureInfo.InvariantCulture, "Submitting Task {0}", _taskContext.CurrentTaskId()));

                activeContext.SubmitTask(finalConfiguration);
            }
            else
            {
                if (_taskContext.TaskCompleted == _taskContext.TotalTasks)
                {
                    LOGGER.Log(Level.Info, "All tasks submitted and completed, active context remian idle");
                    driveStatus = DriverStatus.Idle;
                }
            }
        }

        private IConfiguration GetNextTaskConfiguration()
        {
            string nextTaskId = _taskContext.NextTaskId();
            LOGGER.Log(Level.Info, "GetNextTaskConfiguration, nextTaskId: " + nextTaskId);
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
        private IList<string> taskIds = new List<string>();

        private IDictionary<string, TaskStatus> tasks = new Dictionary<string, TaskStatus>();

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
            Console.WriteLine("NextTaskId: " + NextTaskIndex);
           if (NextTaskIndex < TotalTasks)
           {
               string id = "Jan7DemoTask_" + DateTime.Now.Ticks;
               taskIds.Add(id);
               tasks.Add(id, TaskStatus.Submitting);
               NextTaskIndex++;
               return id;
           }
           return null;
        }

        public string CurrentTaskId()
        {
            Console.WriteLine("CurrentTaskIndex: " + (NextTaskIndex - 1));
            if (NextTaskIndex <= TotalTasks)
            {
                Console.WriteLine("CurrentTaskId: " + taskIds[NextTaskIndex - 1]);
                return taskIds[NextTaskIndex - 1];
            }
            return null; //either not started or completed
        }

        public void UpdateTaskStatus(string taskId, TaskStatus status)
        {
            tasks[taskId] = status;
        }

        public string TaskStatusString()
        {
            Console.WriteLine("TaskStatusString 1, nextTaskIndex: " + NextTaskIndex);
            StringBuilder sb = new StringBuilder();

            if (tasks.Count > 0)
            {
                foreach (var pair in tasks)
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
}
