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
package com.microsoft.reef.runtime.yarn.driver;

import com.microsoft.reef.io.TempFileCreator;
import com.microsoft.reef.io.WorkingDirectoryTempFileCreator;
import com.microsoft.reef.proto.DriverRuntimeProtocol;
import com.microsoft.reef.proto.ReefServiceProtos;
import com.microsoft.reef.runtime.common.driver.api.ResourceLaunchHandler;
import com.microsoft.reef.runtime.common.launch.CLRLaunchCommandBuilder;
import com.microsoft.reef.runtime.common.launch.JavaLaunchCommandBuilder;
import com.microsoft.reef.runtime.common.launch.LaunchCommandBuilder;
import com.microsoft.reef.runtime.yarn.util.YarnUtils;
import com.microsoft.tang.Configuration;
import com.microsoft.tang.InjectionFuture;
import com.microsoft.tang.Tang;
import com.microsoft.tang.annotations.Parameter;
import com.microsoft.tang.formats.ConfigurationSerializer;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

import javax.inject.Inject;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Resource launch handler for YARN.
 */
public final class YARNResourceLaunchHandler implements ResourceLaunchHandler {

  private static final Logger LOG = Logger.getLogger(YARNResourceLaunchHandler.class.getName());
  private final Containers containers;
  private final ConfigurationSerializer configurationSerializer;
  private final FileSystem fileSystem;
  private final String globalClassPath;
  private final Path jobSubmissionDirectory;
  private final Map<String, LocalResource> globalResources = new HashMap<>();
  private final TempFileCreator tempFileCreator;
  private final InjectionFuture<YarnContainerManager> yarnContainerManager;

  @Inject
  YARNResourceLaunchHandler(final @Parameter(YarnMasterConfiguration.GlobalFileClassPath.class) String globalClassPath,
                            final @Parameter(YarnMasterConfiguration.JobSubmissionDirectory.class) String jobSubmissionDirectory,
                            final Containers containers,
                            final ConfigurationSerializer configurationSerializer,
                            final YarnConfiguration yarnConf,
                            final TempFileCreator tempFileCreator,
                            final InjectionFuture<YarnContainerManager> yarnContainerManager) throws IOException {
    LOG.log(Level.FINEST, "Instantiating 'YARNResourceLaunchHandler'");
    this.globalClassPath = globalClassPath;
    this.tempFileCreator = tempFileCreator;
    this.yarnContainerManager = yarnContainerManager;
    this.jobSubmissionDirectory = new Path(jobSubmissionDirectory);
    this.containers = containers;
    this.configurationSerializer = configurationSerializer;
    try {
      this.fileSystem = FileSystem.get(yarnConf);
    } catch (final IOException e) {
      throw new RuntimeException("Unable to instantiate a FileSystem instance.", e);
    }
    final Path globalFilePath = new Path(this.jobSubmissionDirectory, YarnMasterConfiguration.GLOBAL_FILE_DIRECTORY);
    if (this.fileSystem.exists(globalFilePath)) {
      final FileContext fileContext = FileContext.getFileContext(this.fileSystem.getUri());
      setResources(this.fileSystem, this.globalResources, fileContext.listStatus(globalFilePath));
    }
    LOG.log(Level.INFO, "Instantiated 'YARNResourceLaunchHandler'");
  }

  @Override
  public void onNext(final DriverRuntimeProtocol.ResourceLaunchProto resourceLaunchProto) {
    try {

      final String containerId = resourceLaunchProto.getIdentifier();
      LOG.log(Level.FINEST, "TIME: Start ResourceLaunchProto {0}", containerId);

      final Container container = this.containers.get(containerId);

      LOG.log(Level.FINEST, "Setting up container launch container for id={0}", container.getId());

      final Path evaluatorSubmissionDirectory = new Path(this.jobSubmissionDirectory, container.getId().toString());
      final Map<String, LocalResource> localResources = new HashMap<>(this.globalResources);

      // EVALUATOR CONFIGURATION
      final File evaluatorConfigurationFile = this.tempFileCreator.createTempFile("evaluator_" + container.getId(), ".conf");
      LOG.log(Level.FINEST, "TIME: Config ResourceLaunchProto {0} {1}",
          new Object[]{containerId, evaluatorConfigurationFile});

      final Configuration evaluatorConfiguration = Tang.Factory.getTang()
          .newConfigurationBuilder(this.configurationSerializer.fromString(resourceLaunchProto.getEvaluatorConf()))
          .bindImplementation(TempFileCreator.class, WorkingDirectoryTempFileCreator.class)
          .build();

      this.configurationSerializer.toFile(evaluatorConfiguration, evaluatorConfigurationFile);

      localResources.put(evaluatorConfigurationFile.getName(),
          YarnUtils.getLocalResource(this.fileSystem, new Path(evaluatorConfigurationFile.toURI()),
              new Path(evaluatorSubmissionDirectory, evaluatorConfigurationFile.getName()))
      );

      // LOCAL FILE RESOURCES
      LOG.log(Level.FINEST, "TIME: Local ResourceLaunchProto {0}", containerId);
      final StringBuilder localClassPath = new StringBuilder();
      for (final ReefServiceProtos.FileResourceProto file : resourceLaunchProto.getFileList()) {
        final Path src = new Path(file.getPath());
        final Path dst = new Path(this.jobSubmissionDirectory, file.getName());
        switch (file.getType()) {
          case PLAIN:
            if (this.fileSystem.exists(dst)) {
              LOG.log(Level.FINEST, "LOCAL FILE RESOURCE: reference {0}", dst);
              localResources.put(file.getName(), YarnUtils.getLocalResource(this.fileSystem, dst));
            } else {
              LOG.log(Level.FINEST, "LOCAL FILE RESOURCE: upload {0} to {1}", new Object[]{src, dst});
              localResources.put(file.getName(), YarnUtils.getLocalResource(this.fileSystem, src, dst));
            }
            break;
          case LIB:
            localClassPath.append(File.pathSeparatorChar + file.getName());
            if (this.fileSystem.exists(dst)) {
              LOG.log(Level.FINEST, "LOCAL LIB FILE RESOURCE: reference {0}", dst);
              localResources.put(file.getName(), YarnUtils.getLocalResource(this.fileSystem, dst));
            } else {
              LOG.log(Level.FINEST, "LOCAL LIB FILE RESOURCE: upload {0} to {1}", new Object[]{src, dst});
              localResources.put(file.getName(), YarnUtils.getLocalResource(this.fileSystem, src, dst));
            }

            break;
          case ARCHIVE:
            localResources.put(file.getName(), YarnUtils.getLocalResource(this.fileSystem, src, dst));
            break;
        }
      }

      final String classPath = localClassPath.toString().isEmpty() ?
          this.globalClassPath : localClassPath.toString() + File.pathSeparatorChar + this.globalClassPath;

      final LaunchCommandBuilder commandBuilder;
      switch (resourceLaunchProto.getType()) {
        case JVM:
          commandBuilder = new JavaLaunchCommandBuilder().setClassPath(classPath);
          break;
        case CLR:
          commandBuilder = new CLRLaunchCommandBuilder();
          break;
        default:
          throw new IllegalArgumentException("Unsupported container type: " + resourceLaunchProto.getType());
      }

      final List<String> commandList = commandBuilder
          .setErrorHandlerRID(resourceLaunchProto.getRemoteId())
          .setLaunchID(resourceLaunchProto.getIdentifier())
          .setConfigurationFileName(evaluatorConfigurationFile.getName())
          .setMemory(container.getResource().getMemory())
          .setStandardErr(ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/evaluator.stderr")
          .setStandardOut(ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/evaluator.stdout")
          .build();

      final String command = StringUtils.join(commandList, ' ');
      LOG.log(Level.FINEST, "TIME: Run ResourceLaunchProto {0} command: `{1}` with resources: `{2}`",
          new Object[]{containerId, command, localResources});

      final ContainerLaunchContext ctx = YarnUtils.getContainerLaunchContext(command, localResources);
      this.yarnContainerManager.get().submit(container, ctx);

      LOG.log(Level.FINEST, "TIME: End ResourceLaunchProto {0}", containerId);

    } catch (final Throwable e) {
      LOG.log(Level.WARNING, "Error handling resource launch message: " + resourceLaunchProto, e);
      throw new RuntimeException(e);
    }
  }


  private void setResources(final FileSystem fs,
                            final Map<String, LocalResource> resources,
                            final RemoteIterator<FileStatus> files) throws IOException {
    while (files.hasNext()) {
      final FileStatus fstatus = files.next();
      if (fstatus.isFile()) {
        LOG.log(Level.FINEST, "Load file resource: {0}", fstatus.getPath());
        resources.put(fstatus.getPath().getName(), YarnUtils.getLocalResource(fs, fstatus.getPath()));
      } else if (fstatus.isSymlink()) {
        LOG.log(Level.FINEST, "Load symlink resource: {0}", fstatus.getSymlink());
        resources.put(fstatus.getPath().getName(), YarnUtils.getLocalResource(fs, fstatus.getSymlink()));
      }
    }
  }
}
