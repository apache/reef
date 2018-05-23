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
using System.Linq;
using System.Runtime.Serialization;
using Google.Protobuf;
using Grpc.Core;
using Org.Apache.REEF.Bridge.Core.Common.Driver;
using Org.Apache.REEF.Bridge.Core.Proto;
using Org.Apache.REEF.Common.Exceptions;
using Org.Apache.REEF.Driver.Evaluator;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Formats;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Utilities;
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.Bridge.Core.Grpc.Driver
{
    internal class DriverServiceClient : IDriverServiceClient
    {

        private static readonly Logger Logger = Logger.GetLogger(typeof(DriverServiceClient));

        private readonly IConfigurationSerializer _configurationSerializer;

        private readonly DriverService.DriverServiceClient _driverServiceStub;

        [Inject]
        private DriverServiceClient(
            IConfigurationSerializer configurationSerializer,
            [Parameter(Value = typeof(DriverServicePort))] int driverServicePort)
        {
            _configurationSerializer = configurationSerializer;
            Logger.Log(Level.Info, "Binding to driver service at port {0}", driverServicePort);
            var driverServiceChannel = new Channel("127.0.0.1", driverServicePort, ChannelCredentials.Insecure);
            _driverServiceStub = new DriverService.DriverServiceClient(driverServiceChannel);
            Logger.Log(Level.Info, "Channel state {0}", driverServiceChannel.State);
        }

        public void RegisterDriverClientService(Exception exception)
        {
            Logger.Log(Level.Info, $"Register driver client error {exception}");
            var registration = new DriverClientRegistration
            {
                Exception = GrpcUtils.SerializeException(exception)
            };
            _driverServiceStub.RegisterDriverClient(registration);
        }

        public void RegisterDriverClientService(string host, int port)
        {
            Logger.Log(Level.Info, $"Register driver client at host {host}, port {port}");
            var registration = new DriverClientRegistration
            {
                Host = host,
                Port = port
            };
            _driverServiceStub.RegisterDriverClient(registration);
        }

        public void OnShutdown()
        {
            Logger.Log(Level.Info, "Driver clean shutdown");
            _driverServiceStub.Shutdown(new ShutdownRequest());
        }

        public void OnShutdown(Exception ex)
        {
            Logger.Log(Level.Error, "Driver shutdown with error", ex);
            byte[] errorBytes;
            try
            {
                errorBytes = ByteUtilities.SerializeToBinaryFormat(ex);
            }
            catch (SerializationException se)
            {
                Logger.Log(Level.Warning, "Unable to serialize exception", ex);
                errorBytes = ByteUtilities.SerializeToBinaryFormat(
                    NonSerializableJobException.UnableToSerialize(ex, se));
            }

            _driverServiceStub.Shutdown(new ShutdownRequest()
            {
                Exception = new ExceptionInfo()
                {
                    NoError = false,
                    Message = ex.Message,
                    Name = ex.Source,
                    Data = ByteString.CopyFrom(errorBytes)
                }
            });
        }

        public void OnSetAlarm(string alarmId, long timeoutMs)
        {
            _driverServiceStub.SetAlarm(new AlarmRequest()
            {
                AlarmId = alarmId,
                TimeoutMs = (int) timeoutMs
            });
        }

        public void OnEvaluatorRequest(IEvaluatorRequest evaluatorRequest)
        {
            var request = new ResourceRequest()
            {
                ResourceCount = evaluatorRequest.Number,
                Cores = evaluatorRequest.VirtualCore,
                MemorySize = evaluatorRequest.MemoryMegaBytes,
                RelaxLocality = evaluatorRequest.RelaxLocality,
                RuntimeName = evaluatorRequest.RuntimeName,
                NodeLabel = evaluatorRequest.NodeLabelExpression
            };
            if (!string.Empty.Equals(evaluatorRequest.Rack))
            {
                request.RackNameList.Add(evaluatorRequest.Rack);
            }

            if (evaluatorRequest.NodeNames.Count > 0)
            {
                request.NodeNameList.Add(evaluatorRequest.NodeNames);
            }
            _driverServiceStub.RequestResources(request);
        }

        public void OnEvaluatorClose(string evalautorId)
        {
            _driverServiceStub.AllocatedEvaluatorOp(new AllocatedEvaluatorRequest()
            {
                EvaluatorId = evalautorId,
                CloseEvaluator = true
            });
        }

        public void OnEvaluatorSubmit(
            string evaluatorId, 
            IConfiguration contextConfiguration, 
            Optional<IConfiguration> serviceConfiguration, 
            Optional<IConfiguration> taskConfiguration,
            List<FileInfo> addFileList, List<FileInfo> addLibraryList)
        {
            Logger.Log(Level.Info, "Submitting allocated evaluator");

            var evaluatorConf =
                _configurationSerializer.ToString(TangFactory.GetTang().NewConfigurationBuilder().Build());
            var contextConf = _configurationSerializer.ToString(contextConfiguration);
            var serviceConf = !serviceConfiguration.IsPresent()
                ? string.Empty
                : _configurationSerializer.ToString(serviceConfiguration.Value);
            var taskConf = !taskConfiguration.IsPresent()
                ? string.Empty
                : _configurationSerializer.ToString(taskConfiguration.Value);
            var request = new AllocatedEvaluatorRequest()
            {
                EvaluatorId = evaluatorId,
                EvaluatorConfiguration = evaluatorConf,
                ServiceConfiguration = serviceConf,
                ContextConfiguration = contextConf,
                TaskConfiguration = taskConf,
                SetProcess = new AllocatedEvaluatorRequest.Types.EvaluatorProcessRequest()
                {
                    ProcessType = AllocatedEvaluatorRequest.Types.EvaluatorProcessRequest.Types.Type.Dotnet
                }
            };
            request.AddFiles.Add(addFileList.Select(f => f.ToString()));
            request.AddLibraries.Add(addLibraryList.Select(f => f.ToString()));
            _driverServiceStub.AllocatedEvaluatorOp(request);
        }

        public void OnContextClose(string contextId)
        {
            Logger.Log(Level.Info, "Close context {0}", contextId);
            _driverServiceStub.ActiveContextOp(new ActiveContextRequest()
            {
                ContextId = contextId,
                CloseContext = true
            });
        }

        public void OnContextSubmitContext(string contextId, IConfiguration contextConfiguration)
        {
            _driverServiceStub.ActiveContextOp(new ActiveContextRequest()
            {
                ContextId = contextId,
                NewContextRequest = _configurationSerializer.ToString(contextConfiguration)
            });
        }

        public void OnContextSubmitTask(string contextId, IConfiguration taskConfiguration)
        {
            _driverServiceStub.ActiveContextOp(new ActiveContextRequest()
            {
                ContextId = contextId,
                NewTaskRequest = _configurationSerializer.ToString(taskConfiguration)
            });
        }

        public void OnContextMessage(string contextId, byte[] message)
        {
            _driverServiceStub.ActiveContextOp(new ActiveContextRequest()
            {
                ContextId = contextId,
                Message = ByteString.CopyFrom(message)
            });
        }

        public void OnTaskSuspend(string taskId, Optional<byte[]> message)
        {
            var op = new RunningTaskRequest()
            {
                TaskId = taskId,
                Operation = RunningTaskRequest.Types.Operation.Suspend
            };
            if (message.IsPresent())
            {
                op.Message = ByteString.CopyFrom(message.Value);
            }

            _driverServiceStub.RunningTaskOp(op);
        }

        public void OnTaskClose(string taskId, Optional<byte[]> message)
        {
            var op = new RunningTaskRequest()
            {
                TaskId = taskId,
                Operation = RunningTaskRequest.Types.Operation.Close
            };
            if (message.IsPresent())
            {
                op.Message = ByteString.CopyFrom(message.Value);
            }

            _driverServiceStub.RunningTaskOp(op);
        }

        public void OnTaskMessage(string taskId, byte[] message)
        {
            _driverServiceStub.RunningTaskOp(new RunningTaskRequest()
            {
                TaskId = taskId,
                Message = ByteString.CopyFrom(message),
                Operation = RunningTaskRequest.Types.Operation.SendMessage
            });
        }
    }
}
