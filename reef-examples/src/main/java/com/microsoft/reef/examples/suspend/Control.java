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

import com.microsoft.tang.Configuration;
import com.microsoft.tang.Injector;
import com.microsoft.tang.JavaConfigurationBuilder;
import com.microsoft.tang.Tang;
import com.microsoft.tang.annotations.Name;
import com.microsoft.tang.annotations.NamedParameter;
import com.microsoft.tang.annotations.Parameter;
import com.microsoft.tang.exceptions.BindException;
import com.microsoft.tang.formats.CommandLine;
import com.microsoft.wake.EStage;
import com.microsoft.wake.EventHandler;
import com.microsoft.wake.impl.LoggingEventHandler;
import com.microsoft.wake.impl.ThreadPoolStage;
import com.microsoft.wake.remote.impl.ObjectSerializableCodec;
import com.microsoft.wake.remote.impl.TransportEvent;
import com.microsoft.wake.remote.transport.Link;
import com.microsoft.wake.remote.transport.Transport;
import com.microsoft.wake.remote.transport.netty.NettyMessagingTransport;

import javax.inject.Inject;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.logging.Level;
import java.util.logging.Logger;

public final class Control {

  @NamedParameter(doc = "Task id", short_name = "task")
  public static final class TaskId implements Name<String> {
  }

  @NamedParameter(doc = "Command: 'suspend' or 'resume'", short_name = "cmd")
  public static final class Command implements Name<String> {
  }

  private static final Logger LOG = Logger.getLogger(Control.class.getName());

  private final transient String command;
  private final transient String taskId;
  private final transient int port;

  @Inject
  public Control(@Parameter(SuspendClientControl.Port.class) final int port,
                 @Parameter(TaskId.class) final String taskId,
                 @Parameter(Command.class) final String command) {
    this.command = command.trim().toLowerCase();
    this.taskId = taskId;
    this.port = port;
  }

  private static Configuration getConfig(final String[] args) throws IOException, BindException {
    final JavaConfigurationBuilder cb = Tang.Factory.getTang().newConfigurationBuilder();
    new CommandLine(cb).processCommandLine(args, SuspendClientControl.Port.class, TaskId.class, Command.class);
    return cb.build();
  }

  public void run() throws Exception {

    LOG.log(Level.INFO, "command: {0} task: {1} port: {2}",
        new Object[] { this.command, this.taskId, this.port });

    final ObjectSerializableCodec<String> codec = new ObjectSerializableCodec<>();

    final EStage<TransportEvent> stage = new ThreadPoolStage<>("suspend-control-client",
        new LoggingEventHandler<TransportEvent>(), 1, new EventHandler<Throwable>() {
      @Override
      public void onNext(final Throwable throwable) {
        throw new RuntimeException(throwable);
      }
    });

    try (final Transport transport = new NettyMessagingTransport("localhost", 0, stage, stage, 1, 10000)) {
      final Link link = transport.open(new InetSocketAddress("localhost", this.port), codec, null);
      link.write(this.command + " " + this.taskId);
    }
  }

  public static void main(final String[] args) throws Exception {
    final Configuration config = getConfig(args);
    final Injector injector = Tang.Factory.getTang().newInjector(config);
    final Control control = injector.getInstance(Control.class);
    control.run();
  }
}
