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
using Org.Apache.REEF.Common.Tasks;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Network.Elastic.Task;
using Org.Apache.REEF.Network.Elastic.Task.Impl;
using Org.Apache.REEF.Common.Tasks.Events;

namespace Org.Apache.REEF.Network.Examples.Elastic
{
    /// <summary>
    /// Default implementation of a task using the elastic group communication service.
    /// </summary>
    public abstract class DefaultElasticTask : ITask, IObserver<ICloseEvent>
    {
        private readonly IElasticContext _context;
        private readonly IElasticStage _stage;

        private readonly CancellationSource _cancellationSource;

        public DefaultElasticTask(
            CancellationSource source,
            IElasticContext context,
            string stageName)
        {
            _context = context;
            _cancellationSource = source;

            _stage = _context.GetStage(stageName);
        }

        public byte[] Call(byte[] memento)
        {
            _context.WaitForTaskRegistration(_cancellationSource.Source);

            using (var workflow = _stage.Workflow)
            {
                try
                {
                    Execute(memento, workflow);
                }
                catch (Exception e)
                {
                    workflow.Throw(e);
                }
            }

            return null;
        }

        public void Dispose()
        {
            _cancellationSource.Cancel();
            _context.Dispose();
        }

        public void OnNext(ICloseEvent value)
        {
            _stage.Cancel();
        }

        public void OnError(Exception error)
        {
        }

        public void OnCompleted()
        {
        }

        protected abstract void Execute(byte[] memento, Workflow workflow);
    }
}
