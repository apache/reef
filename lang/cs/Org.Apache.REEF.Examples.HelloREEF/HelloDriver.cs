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
using System.Diagnostics;
using System.IO;
using System.Linq;
using Org.Apache.REEF.Common.Files;
using Org.Apache.REEF.Driver;
using Org.Apache.REEF.Driver.Bridge;
using Org.Apache.REEF.Driver.Evaluator;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Tang.Util;
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.Examples.HelloREEF
{
    /// <summary>
    /// The Driver for HelloREEF: It requests a single Evaluator and then submits the HelloTask to it.
    /// </summary>
    public sealed class HelloDriver : IObserver<IAllocatedEvaluator>, IObserver<DateTime>
    {
        private static readonly Logger _Logger = Logger.GetLogger(typeof(HelloDriver));

        /// <summary>
        /// Contexts contain configuration data used beyond a single task.
        /// </summary>
        private static readonly IConfiguration ContextConfiguration =
            Driver.Context.ContextConfiguration.ConfigurationModule
                .Set(Driver.Context.ContextConfiguration.Identifier, "HelloContext")
                .Build();

        /// <summary>
        /// The TaskConfiguration contains the type of Task to run as well as the identifier of that task
        /// </summary>
        private static readonly IConfiguration TaskConfiguration = Common.Tasks.TaskConfiguration.ConfigurationModule
            .Set(Common.Tasks.TaskConfiguration.Identifier, "HelloTask")
            .Set(Common.Tasks.TaskConfiguration.Task, GenericType<HelloTask>.Class)
            .Build();

        private readonly REEFFileNames _fileNames;
        private readonly IEvaluatorRequestor _evaluatorRequestor;

        [Inject]
        private HelloDriver(REEFFileNames fileNames, IEvaluatorRequestor evaluatorRequestor)
        {
            _fileNames = fileNames;
            ClrHandlerHelper.GenerateClassHierarchy(GetGlobalAssemblies());
            _evaluatorRequestor = evaluatorRequestor;
        }

        /// <summary>
        /// Called to start the user mode driver
        /// </summary>
        /// <param name="startTime"></param>
        public void OnNext(DateTime startTime)
        {
            _Logger.Log(Level.Info, string.Format("HelloDriver started at {0}", startTime));
            _evaluatorRequestor.Submit(new EvaluatorRequest(number: 1, megaBytes: 64));
        }

        /// <summary>
        /// Submits the HelloTask to the Evaluator.
        /// </summary>
        /// <param name="allocatedEvaluator"></param>
        public void OnNext(IAllocatedEvaluator allocatedEvaluator)
        {
            allocatedEvaluator.SubmitContextAndTask(ContextConfiguration, TaskConfiguration);
        }

        public void OnError(Exception error)
        {
            throw error;
        }

        public void OnCompleted()
        {
        }

        /// <summary>
        /// </summary>
        /// <returns>All DLLs in the global folder</returns>
        private ISet<string> GetGlobalAssemblies()
        {
            return new HashSet<string>(Directory.GetFiles(_fileNames.GetGlobalFolderPath())
                .Where(e => !(string.IsNullOrWhiteSpace(e)))
                .Select(Path.GetFullPath)
                .Where(File.Exists)
                .Where(IsBinary)
                .Select(Path.GetFileNameWithoutExtension));
        }

        /// <summary>
        /// </summary>
        /// <param name="path"></param>
        /// <returns>True, if the path refers to an EXE or DLL</returns>
        private static Boolean IsBinary(string path)
        {
            if (string.IsNullOrWhiteSpace(path))
            {
                return false;
            }
            var extension = Path.GetExtension(path).ToLower();
            return extension.EndsWith("dll") || extension.EndsWith("exe");
        }
    }
}