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
using System.IO;
using Org.Apache.REEF.Common.Context;
using Org.Apache.REEF.Common.Evaluator;
using Org.Apache.REEF.Driver.Evaluator;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Utilities;

namespace Org.Apache.REEF.Bridge.Core.Common.Driver.Events
{
    internal sealed class BridgeAllocatedEvaluator : IAllocatedEvaluator
    {
        private readonly IDriverServiceClient _driverServiceClient;

        private readonly IEvaluatorDescriptor _evaluatorDescriptor;

        private readonly List<FileInfo> _addFiles = new List<FileInfo>();

        private readonly List<FileInfo> _addLibraries = new List<FileInfo>();

        public string Id { get; }

        public EvaluatorType Type
        {
            get { return EvaluatorType.CLR;  }
            set
            {
                throw new NotImplementedException();
            }
        }

        public string NameServerInfo
        {
            get
            {
                throw new NotImplementedException();
            }
            set
            {
                throw new NotImplementedException();
            }
        }

        public string EvaluatorBatchId
        {
            get
            {
                throw new NotImplementedException();
            }
            set
            {
                throw new NotImplementedException();
            }
        }

        public BridgeAllocatedEvaluator(
            string id,
            IDriverServiceClient driverServiceClient, 
            IEvaluatorDescriptor evaluatorDescriptor)
        {
            Id = id;
            _driverServiceClient = driverServiceClient;
            _evaluatorDescriptor = evaluatorDescriptor;
        }

        public void Dispose()
        {
            _driverServiceClient.OnEvaluatorClose(Id);
        }

       public void SubmitContext(IConfiguration contextConfiguration)
        {
            _driverServiceClient.OnEvaluatorSubmit(
                Id, 
                contextConfiguration, 
                Optional<IConfiguration>.Empty(), 
                Optional<IConfiguration>.Empty(), 
                _addFiles, _addLibraries);
        }

        public void SubmitContextAndService(IConfiguration contextConfiguration, IConfiguration serviceConfiguration)
        {
            _driverServiceClient.OnEvaluatorSubmit(
                Id, 
                contextConfiguration, 
                Optional<IConfiguration>.Of(serviceConfiguration), 
                Optional<IConfiguration>.Empty(), 
                _addFiles, _addLibraries);
        }

        public void SubmitContextAndTask(IConfiguration contextConfiguration, IConfiguration taskConfiguration)
        {
            _driverServiceClient.OnEvaluatorSubmit(
                Id, 
                contextConfiguration,
                Optional<IConfiguration>.Empty(), 
                Optional<IConfiguration>.Of(taskConfiguration), 
                _addFiles, _addLibraries);
        }

        public void SubmitContextAndServiceAndTask(IConfiguration contextConfiguration, IConfiguration serviceConfiguration,
            IConfiguration taskConfiguration)
        {
            _driverServiceClient.OnEvaluatorSubmit(
                Id, 
                contextConfiguration, 
                Optional<IConfiguration>.Of(serviceConfiguration), 
                Optional<IConfiguration>.Of(taskConfiguration), 
                _addFiles, _addLibraries);
        }

        public void SubmitTask(IConfiguration taskConf)
        {
            var contextConfiguration =
                ContextConfiguration.ConfigurationModule.Set(
                    ContextConfiguration.Identifier, "RootContext_" + Id).Build();
            _driverServiceClient.OnEvaluatorSubmit(
                Id, 
                contextConfiguration, 
                Optional<IConfiguration>.Empty(), 
                Optional<IConfiguration>.Of(taskConf), 
                _addFiles, _addLibraries);
        }


        public IEvaluatorDescriptor GetEvaluatorDescriptor()
        {
            return _evaluatorDescriptor;
        }

        public void AddFile(string file)
        {
            _addFiles.Add(new FileInfo(file));
        }

        public void AddLibrary(string lib)
        {
            _addLibraries.Add(new FileInfo(lib));
        }

        public void AddFileResource(string file)
        {
            _addFiles.Add(new FileInfo(file));
        }
    }
}
