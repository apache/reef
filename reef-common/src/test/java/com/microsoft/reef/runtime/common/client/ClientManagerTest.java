/**
 * Copyright (C) 2013 Microsoft Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.microsoft.reef.runtime.common.client;

import com.google.protobuf.ByteString;
import com.microsoft.reef.client.*;
import com.microsoft.reef.proto.ClientRuntimeProtocol;
import com.microsoft.reef.proto.ReefServiceProtos;
import com.microsoft.reef.runtime.common.client.api.JobSubmissionHandler;
import com.microsoft.reef.runtime.common.utils.RemoteManager;
import com.microsoft.reef.util.RuntimeError;
import com.microsoft.tang.InjectionFuture;
import com.microsoft.tang.Injector;
import com.microsoft.wake.EventHandler;
import com.microsoft.wake.remote.RemoteIdentifier;
import com.microsoft.wake.remote.RemoteMessage;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

public class ClientManagerTest {

  private Injector injector;

  private InjectionFuture injectionFuture;

  private RemoteManager remoteManager;

  private JobSubmissionHandler jobSubmissionHandler;

  private JobObserver jobObserver;

  private EventHandler<RuntimeError> runtimeErrorHandler;

  /**
   * Create some basic mockups
   *
   * @throws Exception when something goes wrong
   */
  private final void reset() throws Exception {
    this.injector = Mockito.mock(Injector.class);

    this.remoteManager = Mockito.mock(RemoteManager.class);
    this.jobObserver = Mockito.mock(JobObserver.class);
    this.runtimeErrorHandler = Mockito.mock(EventHandler.class);
    this.jobSubmissionHandler = Mockito.mock(JobSubmissionHandler.class);

    Mockito.when(this.injector.getInstance(JobObserver.class)).thenReturn(this.jobObserver);
    Mockito.when(this.injector.getInstance(EventHandler.class)).thenReturn(this.runtimeErrorHandler);
    Mockito.when(this.injector.getInstance(RunningJob.class)).thenReturn(Mockito.mock(RunningJob.class));
    Mockito.when(this.injector.createChildInjector()).thenReturn(this.injector);

    Mockito.when(this.remoteManager.registerHandler(Mockito.<Class<Object>>any(), Mockito.<EventHandler<RemoteMessage<Object>>>any())).thenReturn(Mockito.mock(AutoCloseable.class));
    Mockito.when(this.remoteManager.registerHandler(Mockito.anyString(), Mockito.<Class<Object>>any(), Mockito.<EventHandler<Object>>any())).thenReturn(Mockito.mock(AutoCloseable.class));
  }

  /**
   * Test the creation of a ClientJobStatusHandler and its receipt of a message.
   *
   * @throws Exception when something goes wrong
   */
  @Test
  public final void SimpleClientManagerTest() throws Exception {
    reset();

    final ReefServiceProtos.JobStatusProto status = ReefServiceProtos.JobStatusProto.newBuilder()
        .setState(ReefServiceProtos.State.INIT)
        .setIdentifier("test")
        .build();

    final ClientManager clientManager = new ClientManager(this.injector, null, this.remoteManager, this.jobSubmissionHandler);
    final RemoteMessage message = Mockito.mock(RemoteMessage.class);

    Mockito.when(message.getIdentifier()).thenReturn(Mockito.mock(RemoteIdentifier.class));
    Mockito.when(message.getMessage()).thenReturn(status);


    clientManager.onNext(message);
  }

  /**
   * Test the creation of a RunningJob. When a RunningJob is created it needs to inform
   * the client via the JobObserver API. This test ensures that the JobObserver is indeed
   * informed in accordance to the status of the job. This handles the code path that occurs
   * when a job is first created.
   *
   * @throws Exception when something goes wrong
   */
  @Test
  public final void TestRunningJobImplJobObserverHandlers() throws Exception {
    reset();

    {
      // Create a RunningJob with status RUNNING
      Mockito.doNothing().when(this.jobObserver).onNext(Mockito.<RunningJob>any());
      Mockito.doThrow(new RuntimeException("Job is not completed!")).when(this.jobObserver).onNext(Mockito.<CompletedJob>any());
      Mockito.doThrow(new RuntimeException("Job is not failed!")).when(this.jobObserver).onError(Mockito.<FailedJob>any());
      final ReefServiceProtos.JobStatusProto status = ReefServiceProtos.JobStatusProto.newBuilder()
          .setState(ReefServiceProtos.State.INIT)
          .setIdentifier("test")
          .build();
      final RunningJob job = new RunningJobImpl(this.remoteManager, this.jobObserver, status, "test");
    }

    {
      // Create a RunningJob with status DONE
      // This should generate a RunningJob, followed by a CompleteJob
      Mockito.doNothing().when(this.jobObserver).onNext(Mockito.<RunningJob>any());
      Mockito.doNothing().when(this.jobObserver).onNext(Mockito.<CompletedJob>any());
      Mockito.doThrow(new RuntimeException("Job is not failed!")).when(this.jobObserver).onError(Mockito.<FailedJob>any());
      final ReefServiceProtos.JobStatusProto status = ReefServiceProtos.JobStatusProto.newBuilder()
          .setState(ReefServiceProtos.State.DONE)
          .setIdentifier("test")
          .build();
      final RunningJob job = new RunningJobImpl(this.remoteManager, this.jobObserver, status, "test");
    }

    {
      // Create a RunningJob with status FAILED
      // This should create a RunningJob, followed by a FailedJob
      Mockito.doThrow(new RuntimeException("Job is not completed!")).when(this.jobObserver).onNext(Mockito.<CompletedJob>any());
      Mockito.doNothing().when(this.jobObserver).onError(Mockito.<FailedJob>any());
      final ReefServiceProtos.JobStatusProto status = ReefServiceProtos.JobStatusProto.newBuilder()
          .setState(ReefServiceProtos.State.FAILED)
          .setIdentifier("test")
          .build();
      final RunningJob job = new RunningJobImpl(this.remoteManager, this.jobObserver, status, "test");
    }
  }

  /**
   * Test the job control channel from Client to Driver. Calls made to the RunningJob API translate down
   * to JobControlProto buffers: this test validates those buffers are being setup properly.
   *
   * @throws Exception when something goes wrong
   */
  @Test
  public final void TestJobControlProto() throws Exception {
    reset();

    final EventHandler<ClientRuntimeProtocol.JobControlProto> jobControlProtoHandler = Mockito.mock(EventHandler.class);

    Mockito.when(this.remoteManager.getHandler(Mockito.anyString(), Mockito.eq(ClientRuntimeProtocol.JobControlProto.class))).thenReturn(jobControlProtoHandler);
    final ReefServiceProtos.JobStatusProto status = ReefServiceProtos.JobStatusProto.newBuilder()
        .setState(ReefServiceProtos.State.INIT)
        .setIdentifier("test")
        .build();
    final RunningJob job = new RunningJobImpl(this.remoteManager, this.jobObserver, status, "test");
    job.close();

    final ArgumentCaptor<ClientRuntimeProtocol.JobControlProto> argument = ArgumentCaptor.forClass(ClientRuntimeProtocol.JobControlProto.class);

    Mockito.verify(jobControlProtoHandler).onNext(argument.capture());
    Assert.assertEquals(argument.getValue().getSignal(), ClientRuntimeProtocol.Signal.SIG_TERMINATE);
    Assert.assertTrue(!argument.getValue().hasMessage());

    final byte[] message = ByteString.copyFromUtf8("test").toByteArray();

    Mockito.reset(jobControlProtoHandler);
    job.close(message);
    Mockito.verify(jobControlProtoHandler).onNext(argument.capture());
    Assert.assertEquals(argument.getValue().getSignal(), ClientRuntimeProtocol.Signal.SIG_TERMINATE);
    Assert.assertTrue(argument.getValue().hasMessage());
    Assert.assertArrayEquals(message, argument.getValue().getMessage().toByteArray());

    Mockito.reset(jobControlProtoHandler);
    job.send(message);
    Mockito.verify(jobControlProtoHandler).onNext(argument.capture());
    Assert.assertTrue(argument.getValue().hasMessage());
    Assert.assertArrayEquals(message, argument.getValue().getMessage().toByteArray());
  }
}
