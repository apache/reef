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

using Google.Protobuf;
using Grpc.Core;
using Org.Apache.REEF.Bridge.Core.Common.Client;
using Org.Apache.REEF.Bridge.Core.Common.Client.Config;
using Org.Apache.REEF.Bridge.Core.Common.Client.Events;
using Org.Apache.REEF.Bridge.Core.Proto;
using Org.Apache.REEF.Common.Client;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Exceptions;
using Org.Apache.REEF.Utilities;
using Org.Apache.REEF.Utilities.Logging;
using System;
using System.Threading;
using System.Threading.Tasks;
using Void = Org.Apache.REEF.Bridge.Core.Proto.Void;

namespace Org.Apache.REEF.Bridge.Core.Grpc.Client
{
    /// <summary>
    /// gRPC based client service.
    /// </summary>
    internal sealed class ClientService : BridgeClient.BridgeClientBase, IClientService
    {
        private static readonly Logger Log = Logger.GetLogger(typeof(ClientService));

        private static readonly Void Void = new Void();

        private readonly IObserver<ISubmittedJob> _submittedJobHandler;

        private readonly IObserver<IRunningJob> _runningJobHandler;

        private readonly IObserver<ICompletedJob> _completedJobHandler;

        private readonly IObserver<IFailedJob> _failedJobHandler;

        private readonly IObserver<IJobMessage> _jobMessageHandler;

        private readonly IObserver<IFailedRuntime> _failedRuntimeHandler;

        private readonly IObserver<IWakeError> _wakeErrorHandler;

        private REEFClient.REEFClientClient _clientStub = null;

        private string _jobId = "unknown";

        [Inject]
        private ClientService(
            [Parameter(Value = typeof(ClientParameters.SubmittedJobHandler))] IObserver<ISubmittedJob> submittedJobHandler,
            [Parameter(Value = typeof(ClientParameters.RunningJobHandler))] IObserver<IRunningJob> runningJobHandler,
            [Parameter(Value = typeof(ClientParameters.CompletedJobHandler))] IObserver<ICompletedJob> completedJobHandler,
            [Parameter(Value = typeof(ClientParameters.FailedJobHandler))] IObserver<IFailedJob> failedJobHandler,
            [Parameter(Value = typeof(ClientParameters.JobMessageHandler))] IObserver<IJobMessage> jobMessageHandler,
            [Parameter(Value = typeof(ClientParameters.FailedRuntimeHandler))] IObserver<IFailedRuntime> failedRuntimeHandler,
            [Parameter(Value = typeof(ClientParameters.WakeErrorHandler))] IObserver<IWakeError> wakeErrorHandler)
        {
            _submittedJobHandler = submittedJobHandler;
            _runningJobHandler = runningJobHandler;
            _completedJobHandler = completedJobHandler;
            _failedJobHandler = failedJobHandler;
            _jobMessageHandler = jobMessageHandler;
            _failedRuntimeHandler = failedRuntimeHandler;
            _wakeErrorHandler = wakeErrorHandler;
            LauncherStatus = LauncherStatus.InitStatus;
        }

        public bool IsDone => LauncherStatus.IsDone;

        public void Reset()
        {
            LauncherStatus = LauncherStatus.InitStatus;
        }

        public LauncherStatus LauncherStatus { get; private set; }

        public void Close(byte[] message = null)
        {
            try
            {
                _clientStub?.DriverControlHandler(new DriverControlOp()
                {
                    JobId = _jobId,
                    Message = message == null ? ByteString.Empty : ByteString.CopyFrom(message),
                    Operation = DriverControlOp.Types.Operation.Close
                });
            }
            catch (Exception e)
            {
                Log.Log(Level.Warning, "exception occurred when trying to close job", e);
            }
            LauncherStatus = LauncherStatus.ForceCloseStatus;
            _clientStub = null;
        }

        public void Send(byte[] message)
        {
            if (_clientStub != null)
            {
                _clientStub.DriverControlHandler(new DriverControlOp()
                {
                    JobId = _jobId,
                    Message = ByteString.CopyFrom(message),
                    Operation = DriverControlOp.Types.Operation.Message
                });
            }
            else
            {
                throw new IllegalStateException("Client service is closed");
            }
        }

        public override Task<Void> RegisterREEFClient(REEFClientRegistration request, ServerCallContext context)
        {
            Log.Log(Level.Info, "REEF Client registered on port {0}", request.Port);
            Channel driverServiceChannel = new Channel("127.0.0.1", (int)request.Port, ChannelCredentials.Insecure);
            _clientStub = new REEFClient.REEFClientClient(driverServiceChannel);
            return Task.FromResult(Void);
        }

        public override Task<Void> JobMessageHandler(JobMessageEvent request, ServerCallContext context)
        {
            Log.Log(Level.Info, "Job message from job id {0}", request.JobId);
            _jobMessageHandler.OnNext(new JobMessage(request.JobId, request.Message.ToByteArray()));
            return Task.FromResult(Void);
        }

        public override Task<Void> JobSumittedHandler(JobSubmittedEvent request, ServerCallContext context)
        {
            Log.Log(Level.Info, "Job id {0} submitted", request.JobId);
            UpdateStatusAndNotify(LauncherStatus.SubmittedStatus);
            _submittedJobHandler.OnNext(new SubmittedJob(request.JobId));
            _jobId = request.JobId;
            return Task.FromResult(Void);
        }

        public override Task<Void> JobRunningHandler(JobRunningEvent request, ServerCallContext context)
        {
            Log.Log(Level.Info, "Job id {0} running", request.JobId);
            UpdateStatusAndNotify(LauncherStatus.RunningStatus);
            _runningJobHandler.OnNext(new RunningJob(this, request.JobId));
            return Task.FromResult(Void);
        }

        public override Task<Void> JobCompletedHandler(JobCompletedEvent request, ServerCallContext context)
        {
            if (IsDone) return Task.FromResult(Void);
            Log.Log(Level.Info, "Job id {0} completed", request.JobId);
            UpdateStatusAndNotify(LauncherStatus.CompletedStatus);
            _completedJobHandler.OnNext(new CompletedJob(request.JobId));
            return Task.FromResult(Void);
        }

        public override Task<Void> JobFailedHandler(JobFailedEvent request, ServerCallContext context)
        {
            if (IsDone) return Task.FromResult(Void);
            Log.Log(Level.Info, "Job id {0} failed on {1}", request.JobId, request.Exception.Name);
            var jobFailedEvent = new FailedJob(request.JobId,
                request.Exception.Message,
                request.Exception.Data.ToByteArray());
            UpdateStatusAndNotify(LauncherStatus.Failed(jobFailedEvent.AsError()));
            _failedJobHandler.OnNext(jobFailedEvent);
            return Task.FromResult(Void);
        }

        public override Task<Void> RuntimeErrorHandler(ExceptionInfo request, ServerCallContext context)
        {
            if (!IsDone)
            {
                Log.Log(Level.Info, "Runtime error {0}", request.Message);
                UpdateStatusAndNotify(LauncherStatus.FailedStatus);
                _failedRuntimeHandler.OnNext(new FailedRuntime(_jobId, request.Message, request.Data.ToByteArray()));
            }

            return Task.FromResult(Void);
        }

        public override Task<Void> WakeErrorHandler(ExceptionInfo request, ServerCallContext context)
        {
            if (!IsDone)
            {
                Log.Log(Level.Info, "Wake error {0}", request.Message);
                UpdateStatusAndNotify(LauncherStatus.FailedStatus);
                _wakeErrorHandler.OnNext(new WakeError(_jobId,
                    request.Message,
                    Optional<byte[]>.Of(request.Data.ToByteArray())));
            }

            return Task.FromResult(Void);
        }

        private void UpdateStatusAndNotify(LauncherStatus status)
        {
            lock (this)
            {
                LauncherStatus = status;
                Monitor.PulseAll(this);
            }
        }
    }
}