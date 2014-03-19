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

import com.microsoft.reef.client.ClientConfigurationOptions;
import com.microsoft.reef.client.DriverConfigurationOptions;
import com.microsoft.reef.client.FailedRuntime;
import com.microsoft.reef.client.REEF;
import com.microsoft.reef.proto.ClientRuntimeProtocol.JobSubmissionProto;
import com.microsoft.reef.proto.ReefServiceProtos;
import com.microsoft.reef.proto.ReefServiceProtos.FileResourceProto;
import com.microsoft.reef.proto.ReefServiceProtos.FileType;
import com.microsoft.reef.proto.ReefServiceProtos.JobStatusProto;
import com.microsoft.reef.proto.ReefServiceProtos.RuntimeErrorProto;
import com.microsoft.reef.runtime.common.client.api.JobSubmissionHandler;
import com.microsoft.reef.runtime.common.utils.RemoteManager;
import com.microsoft.reef.util.EnvironmentUtils;
import com.microsoft.reef.util.JARFileMaker;
import com.microsoft.tang.Configuration;
import com.microsoft.tang.InjectionFuture;
import com.microsoft.tang.Injector;
import com.microsoft.tang.Tang;
import com.microsoft.tang.annotations.Name;
import com.microsoft.tang.annotations.NamedParameter;
import com.microsoft.tang.annotations.Parameter;
import com.microsoft.tang.exceptions.BindException;
import com.microsoft.tang.exceptions.InjectionException;
import com.microsoft.tang.formats.ConfigurationFile;
import com.microsoft.wake.EventHandler;
import com.microsoft.wake.remote.RemoteMessage;

import javax.inject.Inject;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Level;
import java.util.logging.Logger;

public final class ClientManager implements REEF, EventHandler<RemoteMessage<JobStatusProto>> {

  static {
    System.out.println(
        "\nPowered by\n" +
            "     ___________  ______  ______  _______" + "\n" +
            "    /  ______  / /  ___/ /  ___/ /  ____/" + "\n" +
            "   /     _____/ /  /__  /  /__  /  /___" + "\n" +
            "  /  /\\  \\     /  ___/ /  ___/ /  ____/" + "\n" +
            " /  /  \\  \\   /  /__  /  /__  /  /" + "\n" +
            "/__/    \\__\\ /_____/ /_____/ /__/    version " + EnvironmentUtils.getReefVersion() + "\n\n" +
            "From Microsoft CISL\n");
  }

  private static final File TEMP_FOLDER;

  static {
    try {
      TEMP_FOLDER = Files.createTempDirectory("reef-tmp-tempFolder").toFile();
    } catch (final IOException e) {
      throw new RuntimeException("Can't create temporary folder", e);
    }
  }

  private final static Logger LOG = Logger.getLogger(ClientManager.class.getName());

  private final Injector injector;
  private final RemoteManager remoteManager;
  private final JobSubmissionHandler jobSubmissionHandler;
  private final AutoCloseable masterChannel;
  private final AutoCloseable errorChannel;
  private final String userName = System.getProperty("user.name");
  private final Map<String, RunningJobImpl> runningJobMap =
      Collections.synchronizedMap(new HashMap<String, RunningJobImpl>());

  @Inject
  ClientManager(final Injector injector,
                final @Parameter(ClientConfigurationOptions.RuntimeErrorHandler.class)
                InjectionFuture<EventHandler<FailedRuntime>> runtimeErrorHandlerFuture,
                final RemoteManager remoteManager,
                final JobSubmissionHandler jobSubmissionHandler) {

    this.injector = injector;
    this.remoteManager = remoteManager;
    this.jobSubmissionHandler = jobSubmissionHandler;

    this.masterChannel = this.remoteManager.registerHandler(JobStatusProto.class, this);
    this.errorChannel = this.remoteManager.registerHandler(RuntimeErrorProto.class,
        new RuntimeErrorProtoHandler(runtimeErrorHandlerFuture, this.runningJobMap));
  }

  @Override
  public final void close() {

    if (this.runningJobMap.size() > 0) {
      LOG.log(Level.WARNING, "unclean shutdown: {0} jobs still running.", this.runningJobMap.size());
      for (final RunningJobImpl runningJob : this.runningJobMap.values()) {
        LOG.log(Level.WARNING, "Force close job {0}", runningJob.getId());
        runningJob.close();
      }
    }

    // This pushes the close into a separate thread in the absence of a fix for Wake-78
    final Runnable r = new Runnable() {
      @Override
      public void run() {
        LOG.log(Level.FINEST, "REEF client manager closing");
        try {
          ClientManager.this.masterChannel.close();
        } catch (final Exception e) {
          LOG.log(Level.SEVERE, "Unable to close masterChannel", e);
        }

        try {
          ClientManager.this.errorChannel.close();
        } catch (final Exception e) {
          LOG.log(Level.SEVERE, "Unable to close errorChannel", e);
        }

        try {
          ClientManager.this.remoteManager.close();
        } catch (final Exception e) {
          LOG.log(Level.SEVERE, "Unable to close remoteManager", e);
        }

        ClientManager.this.jobSubmissionHandler.close();
        LOG.log(Level.FINEST, "REEF client manager closed");
      }
    };
    final ExecutorService ex = Executors.newSingleThreadExecutor();
    ex.submit(r);
    ex.shutdown();
  }

  @Override
  public void submit(final Configuration driverConf) {
    final Injector injector = Tang.Factory.getTang().newInjector(driverConf);
    try {
      final JobSubmissionProto.Builder jbuilder = JobSubmissionProto.newBuilder()
          .setIdentifier(injector.getNamedInstance(DriverConfigurationOptions.DriverIdentifier.class))
          .setRemoteId(this.remoteManager.getMyIdentifier())
          .setUserName(this.userName)
          .setDriverSize(ReefServiceProtos.SIZE.valueOf(injector.getNamedInstance(DriverConfigurationOptions.DriverSize.class)))
          .setConfiguration(ConfigurationFile.toConfigurationString(driverConf));

      for (final String globalFileName : injector.getNamedInstance(DriverConfigurationOptions.GlobalFiles.class)) {
        LOG.log(Level.FINEST,  "Adding global file: {0}", globalFileName);
        jbuilder.addGlobalFile(getFileResourceProto(globalFileName, FileType.PLAIN));
      }

      for (final String globalLibraryName : injector.getNamedInstance(DriverConfigurationOptions.GlobalLibraries.class)) {
        LOG.log(Level.FINEST,  "Adding global library: {0}", globalLibraryName);
        jbuilder.addGlobalFile(getFileResourceProto(globalLibraryName, FileType.LIB));
      }

      for (final String localFileName : injector.getNamedInstance(DriverConfigurationOptions.LocalFiles.class)) {
        LOG.log(Level.FINEST,  "Adding local file: {0}", localFileName);
        jbuilder.addLocalFile(getFileResourceProto(localFileName, FileType.PLAIN));
      }

      for (final String localLibraryName : injector.getNamedInstance(DriverConfigurationOptions.LocalLibraries.class)) {
        LOG.log(Level.FINEST,  "Adding local library: {0}", localLibraryName);
        jbuilder.addLocalFile(getFileResourceProto(localLibraryName, FileType.LIB));
      }

      this.jobSubmissionHandler.onNext(jbuilder.build());

    } catch (final Exception e) {
      LOG.log(Level.SEVERE, "Exception while processing driver configuration.", e);
      try {
        this.masterChannel.close();
        this.errorChannel.close();
      } catch (final Exception e1) {
        LOG.log(Level.SEVERE, "Exception while closing communication channels.", e1);
      }
      throw new RuntimeException(e);
    }
  }

  private final FileResourceProto getFileResourceProto(
      final String fileName, final FileType type) throws IOException {
    File file = new File(fileName);
    if (file.exists()) {
      // It is a local file and can be added.
      if (file.isDirectory()) {
        // If it is a directory, create a JAR file of it and add that instead.
        file = toJar(file);
      }
      return FileResourceProto.newBuilder()
          .setName(file.getName())
          .setPath(file.getPath())
          .setType(type)
          .build();
    } else {
      // The file isn't in the local filesytem. Assume that the file is actually a URI.
      // We then assume that the underlying resource manager knows how to deal with it.
      try {
        final URI uri = new URI(fileName);
        final String path = uri.getPath();
        final String name = path.substring(path.lastIndexOf('/') + 1);
        return FileResourceProto.newBuilder()
            .setName(name)
            .setPath(uri.toString())
            .setType(type)
            .build();
      } catch (final URISyntaxException e) {
        throw new IOException("Unable to parse URI.", e);
      }
    }
  }

  @Override
  public synchronized final void onNext(final RemoteMessage<JobStatusProto> message) {
    final JobStatusProto status = message.getMessage();
    try {
      if (status.getState() == ReefServiceProtos.State.INIT) {
        assert (!this.runningJobMap.containsKey(status.getIdentifier()));
        LOG.log(Level.FINEST,  "Initializing running job {0}", status.getIdentifier());
        final Injector child = this.injector.forkInjector();
        child.bindVolatileParameter(DriverRemoteIdentifier.class, message.getIdentifier().toString());
        child.bindVolatileInstance(JobStatusProto.class, status);

        final RunningJobImpl runningJob = child.getInstance(RunningJobImpl.class);
        this.runningJobMap.put(status.getIdentifier(), runningJob);
        LOG.log(Level.FINEST, "Launched running job {0}", status.getIdentifier());
      } else if (this.runningJobMap.containsKey(status.getIdentifier())) {
        this.runningJobMap.get(status.getIdentifier()).onNext(status);
        if (status.getState() != ReefServiceProtos.State.RUNNING) {
          this.runningJobMap.remove(status.getIdentifier());
        }
      } else {
        throw new RuntimeException("Unknown running job status: " + status);
      }
    } catch (final BindException | InjectionException configError) {
      LOG.log(Level.WARNING, "Configuration error for: " + status, configError);
      try {
        this.masterChannel.close();
      } catch (final Exception ex) {
        LOG.log(Level.WARNING, "Could not close master channel for: " + status, ex);
      }
      throw new RuntimeException("Configuration error for: " + status, configError);
    }
  }

  @NamedParameter(doc = "The driver remote identifier.")
  public final static class DriverRemoteIdentifier implements Name<String> {
  }

  /**
   * Turns temporary folder "foo" into a jar file "foo.jar"
   *
   * @param file
   * @return
   * @throws IOException
   */
  private static File toJar(final File file) throws IOException {
    final File jarFile = File.createTempFile(file.getName(), ".jar", TEMP_FOLDER);
    LOG.log(Level.FINEST, "Adding contents of folder {0} to {1}", new Object[]{file, jarFile});
    try (final JARFileMaker jarMaker = new JARFileMaker(jarFile)) {
      jarMaker.addChildren(file);
    }
    return jarFile;
  }
}
