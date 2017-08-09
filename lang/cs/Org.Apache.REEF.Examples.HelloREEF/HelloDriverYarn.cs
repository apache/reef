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
using System.Linq;
using Org.Apache.REEF.Common.Tasks;
using Org.Apache.REEF.Driver;
using Org.Apache.REEF.Driver.Evaluator;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Util;
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.Examples.HelloREEF
{
    /// <summary>
    /// The Driver for HelloREEF: It requests a single Evaluator and then submits the HelloTask to it.
    /// </summary>
    public sealed class HelloDriverYarn : IObserver<IAllocatedEvaluator>, IObserver<IDriverStarted>
    {
        private static readonly Logger Logger = Logger.GetLogger(typeof(HelloDriver));
        private readonly IEvaluatorRequestor _evaluatorRequestor;

        /// <summary>
        /// List of node names for desired evaluators
        /// </summary>
        private readonly IList<string> _nodeNames;

        /// <summary>
        /// Specify if the desired node names is relaxed
        /// </summary>
        private readonly bool _relaxLocality;

        /// <summary>
        /// Constructor of the driver
        /// </summary>
        /// <param name="evaluatorRequestor">Evaluator Requestor</param>
        /// <param name="nodeNames">Node names for evaluators</param>
        /// <param name="relaxLocality">Relax indicator of evaluator node request</param>
        [Inject]
        private HelloDriverYarn(IEvaluatorRequestor evaluatorRequestor,
            [Parameter(typeof(NodeNames))] ISet<string> nodeNames,
            [Parameter(typeof(RelaxLocality))] bool relaxLocality)
        {
            _evaluatorRequestor = evaluatorRequestor;
            _nodeNames = nodeNames.ToList();
            _relaxLocality = relaxLocality;
        }

        /// <summary>
        /// Submits the HelloTask to the Evaluator.
        /// </summary>
        /// <param name="allocatedEvaluator"></param>
        public void OnNext(IAllocatedEvaluator allocatedEvaluator)
        {
            Logger.Log(Level.Info, "Received allocatedEvaluator-HostName: " + allocatedEvaluator.GetEvaluatorDescriptor().NodeDescriptor.HostName);
            var taskConfiguration = TaskConfiguration.ConfigurationModule
                .Set(TaskConfiguration.Identifier, "HelloTask")
                .Set(TaskConfiguration.Task, GenericType<HelloTask>.Class)
                .Build();
            allocatedEvaluator.SubmitTask(taskConfiguration);
        }

        public void OnError(Exception error)
        {
            throw error;
        }

        public void OnCompleted()
        {
        }

        /// <summary>
        /// Called to start the user mode driver
        /// </summary>
        /// <param name="driverStarted"></param>
        public void OnNext(IDriverStarted driverStarted)
        {
            Logger.Log(Level.Info, string.Format("HelloDriver started at {0}", driverStarted.StartTime));

            if (_nodeNames != null && _nodeNames.Count > 0)
            {
                _evaluatorRequestor.Submit(_evaluatorRequestor.NewBuilder()
                    .AddNodeNames(_nodeNames)
                    .SetMegabytes(64)
                    .SetNumber(_nodeNames.Count)
                    .SetRelaxLocality(_relaxLocality)
                    .Build());
            }
            else
            {
                _evaluatorRequestor.Submit(_evaluatorRequestor.NewBuilder()
                    .SetMegabytes(64)
                    .Build());
            }
        }
    }

    [NamedParameter(documentation: "Set of node names for evaluators")]
    internal class NodeNames : Name<ISet<string>>
    {    
    }

    [NamedParameter(documentation: "RelaxLocality for specifying evaluator node names", shortName: "RelaxLocality", defaultValue: "true")]
    internal class RelaxLocality : Name<bool>
    {        
    }
}