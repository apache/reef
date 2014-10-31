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
package org.apache.reef.examples.suspend;

import org.apache.reef.client.RunningJob;
import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.wake.EStage;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.impl.ThreadPoolStage;
import org.apache.reef.wake.remote.impl.ObjectSerializableCodec;
import org.apache.reef.wake.remote.impl.TransportEvent;
import org.apache.reef.wake.remote.transport.Transport;
import org.apache.reef.wake.remote.transport.netty.NettyMessagingTransport;

import javax.inject.Inject;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * (Wake) listener to get suspend/resume commands from Control process.
 */
public class SuspendClientControl implements AutoCloseable {

  private static final Logger LOG = Logger.getLogger(Control.class.getName());
  private static final ObjectSerializableCodec<byte[]> CODEC = new ObjectSerializableCodec<>();
  private final transient Transport transport;
  private transient RunningJob runningJob;

  @Inject
  public SuspendClientControl(
      final @Parameter(SuspendClientControl.Port.class) int port) throws IOException {

    LOG.log(Level.INFO, "Listen to control port {0}", port);

    final EStage<TransportEvent> stage = new ThreadPoolStage<>(
        "suspend-control-server", new ControlMessageHandler(), 1, new EventHandler<Throwable>() {
      @Override
      public void onNext(final Throwable throwable) {
        throw new RuntimeException(throwable);
      }
    });

    this.transport = new NettyMessagingTransport("localhost", port, stage, stage, 1, 10000);
  }

  public synchronized void setRunningJob(final RunningJob job) {
    this.runningJob = job;
  }

  @Override
  public void close() throws Exception {
    this.transport.close();
  }

  @NamedParameter(doc = "Port for suspend/resume control commands",
      short_name = "port", default_value = "7008")
  public static final class Port implements Name<Integer> {
  }

  /**
   * Forward remote message to the job driver.
   */
  private class ControlMessageHandler implements EventHandler<TransportEvent> {
    @Override
    public synchronized void onNext(final TransportEvent msg) {
      LOG.log(Level.INFO, "Control message: {0} destination: {1}",
          new Object[]{CODEC.decode(msg.getData()), runningJob});
      if (runningJob != null) {
        runningJob.send(msg.getData());
      }
    }
  }
}
