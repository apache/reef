/*
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

import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.JavaConfigurationBuilder;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.exceptions.BindException;
import org.apache.reef.tang.formats.CommandLine;
import org.apache.reef.wake.EStage;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.impl.LoggingEventHandler;
import org.apache.reef.wake.impl.ThreadPoolStage;
import org.apache.reef.wake.remote.impl.ObjectSerializableCodec;
import org.apache.reef.wake.remote.impl.TransportEvent;
import org.apache.reef.wake.remote.transport.Link;
import org.apache.reef.wake.remote.transport.Transport;
import org.apache.reef.wake.remote.transport.TransportFactory;

import javax.inject.Inject;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Control process which sends suspend/resume commands.
 */
public final class Control {

  private static final Logger LOG = Logger.getLogger(Control.class.getName());
  private final transient String command;
  private final transient String taskId;
  private final transient int port;
  private final TransportFactory tpFactory;

  @Inject
  public Control(@Parameter(SuspendClientControl.Port.class) final int port,
                 @Parameter(TaskId.class) final String taskId,
                 @Parameter(Command.class) final String command,
                 final TransportFactory tpFactory) {
    this.command = command.trim().toLowerCase();
    this.taskId = taskId;
    this.port = port;
    this.tpFactory = tpFactory;
  }

  private static Configuration getConfig(final String[] args) throws IOException, BindException {
    final JavaConfigurationBuilder cb = Tang.Factory.getTang().newConfigurationBuilder();
    new CommandLine(cb).processCommandLine(args, SuspendClientControl.Port.class, TaskId.class, Command.class);
    return cb.build();
  }

  public static void main(final String[] args) throws Exception {
    final Configuration config = getConfig(args);
    final Injector injector = Tang.Factory.getTang().newInjector(config);
    final Control control = injector.getInstance(Control.class);
    control.run();
  }

  public void run() throws Exception {

    LOG.log(Level.INFO, "command: {0} task: {1} port: {2}",
        new Object[]{this.command, this.taskId, this.port});

    final ObjectSerializableCodec<String> codec = new ObjectSerializableCodec<>();

    final EStage<TransportEvent> stage = new ThreadPoolStage<>("suspend-control-client",
        new LoggingEventHandler<TransportEvent>(), 1, new EventHandler<Throwable>() {
          @Override
          public void onNext(final Throwable throwable) {
            throw new RuntimeException(throwable);
          }
        });

    try (Transport transport = tpFactory.newInstance("localhost", 0, stage, stage, 1, 10000)) {
      final Link link = transport.open(new InetSocketAddress("localhost", this.port), codec, null);
      link.write(this.command + " " + this.taskId);
    }
  }

  /**
   * Task id.
   */
  @NamedParameter(doc = "Task id", short_name = "task")
  public static final class TaskId implements Name<String> {
  }

  /**
   * Command: 'suspend' or 'resume'.
   */
  @NamedParameter(doc = "Command: 'suspend' or 'resume'", short_name = "cmd")
  public static final class Command implements Name<String> {
  }
}
