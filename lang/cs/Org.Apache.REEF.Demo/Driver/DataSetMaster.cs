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
using Org.Apache.REEF.Common.Context;
using Org.Apache.REEF.Common.Services;
using Org.Apache.REEF.Demo.Task;
using Org.Apache.REEF.Driver.Context;
using Org.Apache.REEF.Driver.Evaluator;
using Org.Apache.REEF.IO.FileSystem;
using Org.Apache.REEF.IO.PartitionedData;
using Org.Apache.REEF.IO.PartitionedData.FileSystem;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Formats;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Tang.Util;
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.Demo.Driver
{
    public sealed class DataSetMaster : IDataSetMaster, IObserver<IAllocatedEvaluator>, IObserver<IActiveContext>
    {
        private static readonly Logger Logger = Logger.GetLogger(typeof(DataSetMaster));

        private readonly IEvaluatorRequestor _evaluatorRequestor;
        private readonly string _serializerConfString;
        private readonly IInjector _injector;
        private readonly Stack<string> _partitionDescriptorIds = new Stack<string>();

        private IPartitionedInputDataSet _partitionedInputDataSet;
        

        [Inject]
        private DataSetMaster(IEvaluatorRequestor evaluatorRequestor,
                              AvroConfigurationSerializer avroConfigurationSerializer,
                              IInjector injector)
        {
            _evaluatorRequestor = evaluatorRequestor;

            IConfiguration serializerConf = TangFactory.GetTang().NewConfigurationBuilder()
                .BindImplementation(GenericType<IFileDeSerializer<IEnumerable<byte>>>.Class,
                    GenericType<ByteSerializer>.Class)
                .Build();
            _serializerConfString = avroConfigurationSerializer.ToString(serializerConf);
            _injector = injector;
        }

        public IDataSet<T> Load<T>(Uri uri)
        {
            var tmpConfModule = FileSystemInputPartitionConfiguration<IEnumerable<byte>>.ConfigurationModule
                .Set(FileSystemInputPartitionConfiguration<IEnumerable<byte>>.FileSerializerConfig, _serializerConfString)
                .Set(FileSystemInputPartitionConfiguration<IEnumerable<byte>>.CopyToLocal, "false");

            var partitionUris = GetPartitionUris(uri);
            foreach (var partitionUri in partitionUris)
            {
                tmpConfModule =
                    tmpConfModule.Set(FileSystemInputPartitionConfiguration<IEnumerable<byte>>.FilePathForPartitions,
                        partitionUri);
            }

            _partitionedInputDataSet =
                    _injector.ForkInjector(tmpConfModule.Build()).GetInstance<IPartitionedInputDataSet>();

            foreach (var descriptor in _partitionedInputDataSet)
            {
                _partitionDescriptorIds.Push(descriptor.Id);
            }

            _evaluatorRequestor.Submit(_evaluatorRequestor.NewBuilder()
                .SetNumber(partitionUris.Length)
                .SetMegabytes(1024)
                .Build());

            return null; // must return IDataSet
        }

        void IObserver<IAllocatedEvaluator>.OnNext(IAllocatedEvaluator allocatedEvaluator)
        {
            Logger.Log(Level.Info, "Got {0}", allocatedEvaluator);
            string descriptorId;
            lock (_partitionDescriptorIds)
            {
                descriptorId = _partitionDescriptorIds.Pop();
            }
            IPartitionDescriptor partitionDescriptor = _partitionedInputDataSet.GetPartitionDescriptorForId(descriptorId);
            IConfiguration contextConf = ContextConfiguration.ConfigurationModule
                .Set(ContextConfiguration.Identifier, "Context-" + descriptorId)
                .Build();
            allocatedEvaluator.SubmitContextAndService(contextConf, partitionDescriptor.GetPartitionConfiguration());
        }

        void IObserver<IActiveContext>.OnNext(IActiveContext activeContext)
        {
            Logger.Log(Level.Info, "Got {0}", activeContext);
            activeContext.Dispose();
        }

        public void Store<T>(IDataSet<T> dataSet)
        {
            throw new NotImplementedException();
        }

        public void OnCompleted()
        {
        }

        public void OnError(Exception e)
        {
            throw e;
        }

        private string[] GetPartitionUris(Uri uri)
        {
            throw new NotImplementedException();
        }
    }
}