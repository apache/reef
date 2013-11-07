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
import com.microsoft.wake.EventHandler;
import com.microsoft.wake.remote.RemoteIdentifier;
import com.microsoft.wake.remote.RemoteIdentifierFactory;
import com.microsoft.wake.remote.RemoteManager;
import com.microsoft.wake.remote.impl.DefaultRemoteIdentifierFactoryImplementation;
import com.microsoft.wake.remote.impl.DefaultRemoteManagerImplementation;
import com.microsoft.wake.remote.impl.ObjectSerializableCodec;

import javax.inject.Inject;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

public final class Control {

  @NamedParameter(doc = "Activity id", short_name = "activity")
  public static final class ActivityId implements Name<String> {
  }

  @NamedParameter(doc = "Command: 'suspend' or 'resume'", short_name = "cmd")
  public static final class Command implements Name<String> {
  }

  private static final Logger LOG = Logger.getLogger(Control.class.getName());

  private final transient String command;
  private final transient String activityId;
  private final transient int port;

  @Inject
  public Control(@Parameter(SuspendClientControl.Port.class) final int port,
                 @Parameter(ActivityId.class) final String activityId,
                 @Parameter(Command.class) final String command) {
    this.command = command.trim().toLowerCase();
    this.activityId = activityId;
    this.port = port;
  }

  public void run() throws Exception {
    LOG.log(Level.INFO, "command: {0} activity: {1} port: {2}",
        new Object[]{this.command, this.activityId, this.port});
    final ObjectSerializableCodec<String> codec = new ObjectSerializableCodec<>();
    try (final RemoteManager rm = new DefaultRemoteManagerImplementation("localhost", 0, codec, new EventHandler<Throwable>() {
      @Override
      public void onNext(final Throwable throwable) {
        throw new RuntimeException(throwable);
      }
    }, true)) {
      final RemoteIdentifierFactory factory = new DefaultRemoteIdentifierFactoryImplementation();
      final RemoteIdentifier remoteId = factory.getNewInstance("socket://localhost:" + port);
      final EventHandler<String> proxyConnection = rm.getHandler(remoteId, String.class);
      proxyConnection.onNext(command + " " + activityId);
    }
  }

  private static Configuration getConfig(final String[] args) throws IOException, BindException {
    final JavaConfigurationBuilder cb = Tang.Factory.getTang().newConfigurationBuilder();
    new CommandLine(cb).processCommandLine(args, SuspendClientControl.Port.class, ActivityId.class, Command.class);
    return cb.build();
  }

  public static void main(final String[] args) throws Exception {
    final Configuration config = getConfig(args);
    final Injector injector = Tang.Factory.getTang().newInjector(config);
    final Control control = injector.getInstance(Control.class);
    control.run();
  }
}
