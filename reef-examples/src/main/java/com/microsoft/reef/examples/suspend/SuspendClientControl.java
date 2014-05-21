/**
 * Copyright (C) 2014 Microsoft Corporation
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
package com.microsoft.reef.examples.suspend;

import com.microsoft.reef.client.RunningJob;
import com.microsoft.tang.annotations.Name;
import com.microsoft.tang.annotations.NamedParameter;
import com.microsoft.tang.annotations.Parameter;
import com.microsoft.wake.EventHandler;
import com.microsoft.wake.remote.impl.ObjectSerializableCodec;
import com.microsoft.wake.remote.impl.RemoteEvent;
import com.microsoft.wake.remote.impl.RemoteReceiverStage;
import com.microsoft.wake.remote.transport.Transport;
import com.microsoft.wake.remote.transport.netty.NettyMessagingTransport;

import javax.inject.Inject;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * (Wake) listener to get suspend/resume commands from Control process.
 */
public class SuspendClientControl implements AutoCloseable {

  @NamedParameter(doc = "Port for suspend/resume control commands",
      short_name = "port", default_value = "7008")
  public static final class Port implements Name<Integer> {
  }

  private static final Logger LOG = Logger.getLogger(Control.class.getName());
  private static final ObjectSerializableCodec<String> CODEC = new ObjectSerializableCodec<>();

  private transient RunningJob runningJob;
  private final transient Transport transport;

  @Inject
  public SuspendClientControl(@Parameter(SuspendClientControl.Port.class) final int port) {
    LOG.log(Level.INFO, "Listen to control port {0}", port);
    final RemoteReceiverStage recvStage = new RemoteReceiverStage(new ControlMessageHandler(), true);
    this.transport = new NettyMessagingTransport("localhost", port, recvStage, recvStage);
  }

  /**
   * Forward remote message to the job driver.
   */
  private class ControlMessageHandler implements EventHandler<RemoteEvent<byte[]>> {
    @Override
    public synchronized void onNext(final RemoteEvent<byte[]> msg) {
      LOG.log(Level.INFO, "Control message: {0} destination: {1}",
              new Object[] { CODEC.decode(msg.getEvent()), runningJob });
      if (runningJob != null) {
        runningJob.send(msg.getEvent());
      }
    }
  }

  public synchronized void setRunningJob(final RunningJob job) {
    this.runningJob = job;
  }

  @Override
  public void close() throws Exception {
    this.transport.close();
  }
}
