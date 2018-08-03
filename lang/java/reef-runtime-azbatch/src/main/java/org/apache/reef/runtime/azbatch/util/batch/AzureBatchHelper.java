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
package org.apache.reef.runtime.azbatch.util.batch;

import com.microsoft.azure.batch.BatchClient;
import com.microsoft.azure.batch.protocol.models.*;

import org.apache.reef.runtime.azbatch.parameters.*;
import org.apache.reef.runtime.azbatch.util.AzureBatchFileNames;
import org.apache.reef.runtime.azbatch.util.command.CommandBuilder;
import org.apache.reef.runtime.azbatch.util.storage.SharedAccessSignatureCloudBlobClientProvider;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.wake.remote.address.ContainerBasedLocalAddressProvider;
import org.apache.reef.wake.remote.ports.parameters.TcpPortSet;

import javax.inject.Inject;
import java.io.IOException;
import java.net.URI;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A helper class for Azure Batch.
 */
public final class AzureBatchHelper {

  private static final Logger LOG = Logger.getLogger(AzureBatchHelper.class.getName());

  /*
   * Environment variable that contains the Azure Batch jobId.
   */
  private static final String AZ_BATCH_JOB_ID_ENV = "AZ_BATCH_JOB_ID";
  private static final String CAPTURE_HOST_IP_ADDRESS_TASK_NAME = "CaptureHostIpAddress";

  private final AzureBatchFileNames azureBatchFileNames;

  private final BatchClient client;
  private final PoolInformation poolInfo;
  private final CommandBuilder commandBuilder;
  private final ContainerRegistryProvider containerRegistryProvider;
  private final boolean areContainersEnabled;
  private final Set<Integer> ports;

  @Inject
  public AzureBatchHelper(
      final AzureBatchFileNames azureBatchFileNames,
      final IAzureBatchCredentialProvider credentialProvider,
      final CommandBuilder commandBuilder,
      final ContainerRegistryProvider containerRegistryProvider,
      @Parameter(TcpPortSet.class) final Set<Integer> ports,
      @Parameter(AzureBatchPoolId.class) final String azureBatchPoolId) {
    this.azureBatchFileNames = azureBatchFileNames;

    this.client = BatchClient.open(credentialProvider.getCredentials());
    this.poolInfo = new PoolInformation().withPoolId(azureBatchPoolId);
    this.commandBuilder = commandBuilder;
    this.containerRegistryProvider = containerRegistryProvider;
    this.ports = ports;
    this.areContainersEnabled = this.containerRegistryProvider.isValid();
  }

  /**
   * Create a job on Azure Batch.
   *
   * @param applicationId           the ID of the application.
   * @param storageContainerSAS     the publicly accessible uri to the job container.
   * @param jobJarUri               the publicly accessible uri to the job jar directory.
   * @param command                 the commandline argument to execute the job.
   * @throws IOException
   */
  public void submitJob(final String applicationId, final String storageContainerSAS, final URI jobJarUri,
                        final String command) throws IOException {
    final ResourceFile jarResourceFile = new ResourceFile()
        .withBlobSource(jobJarUri.toString())
        .withFilePath(AzureBatchFileNames.getTaskJarFileName());

    // This setting will signal Batch to generate an access token and pass it to the Job Manager Task (aka the Driver)
    // as an environment variable.
    // See https://docs.microsoft.com/en-us/dotnet/api/microsoft.azure.batch.cloudtask.authenticationtokensettings
    // for more info.
    final AuthenticationTokenSettings authenticationTokenSettings = new AuthenticationTokenSettings()
        .withAccess(Collections.singletonList(AccessScope.JOB));

    final EnvironmentSetting environmentSetting = new EnvironmentSetting()
        .withName(SharedAccessSignatureCloudBlobClientProvider.AZURE_STORAGE_CONTAINER_SAS_TOKEN_ENV)
        .withValue(storageContainerSAS);

    final JobManagerTask jobManagerTask = new JobManagerTask()
        .withRunExclusive(false)
        .withId(applicationId)
        .withResourceFiles(Collections.singletonList(jarResourceFile))
        .withEnvironmentSettings(Collections.singletonList(environmentSetting))
        .withAuthenticationTokenSettings(authenticationTokenSettings)
        .withKillJobOnCompletion(true)
        .withContainerSettings(createTaskContainerSettings(applicationId))
        .withCommandLine(command);

    LOG.log(Level.INFO, "Job Manager (aka driver) task command: {0}", command);

    final JobAddParameter jobAddParameter = new JobAddParameter()
        .withId(applicationId)
        .withJobManagerTask(jobManagerTask)
        .withJobPreparationTask(createJobPreparationTask())
        .withPoolInfo(poolInfo);

    client.jobOperations().createJob(jobAddParameter);
  }

  /**
   * Adds a single task to a job on Azure Batch.
   *
   * @param jobId     the ID of the job.
   * @param taskId    the ID of the task.
   * @param jobJarUri the publicly accessible uri list to the job jar directory.
   * @param confUri   the publicly accessible uri list to the job configuration directory.
   * @param command   the commandline argument to execute the job.
   * @throws IOException
   */
  public void submitTask(final String jobId, final String taskId, final URI jobJarUri,
                         final URI confUri, final String command)
      throws IOException {

    final List<ResourceFile> resources = new ArrayList<>();

    final ResourceFile jarSourceFile = new ResourceFile()
        .withBlobSource(jobJarUri.toString())
        .withFilePath(AzureBatchFileNames.getTaskJarFileName());
    resources.add(jarSourceFile);

    final ResourceFile confSourceFile = new ResourceFile()
        .withBlobSource(confUri.toString())
        .withFilePath(this.azureBatchFileNames.getEvaluatorShimConfigurationPath());
    resources.add(confSourceFile);

    LOG.log(Level.INFO, "Evaluator task command: {0}", command);

    TaskAddParameter taskAddParameter = new TaskAddParameter()
        .withId(taskId)
        .withResourceFiles(resources)
        .withContainerSettings(createTaskContainerSettings(taskId))
        .withCommandLine(command);

    if (this.areContainersEnabled) {
      taskAddParameter = taskAddParameter.withUserIdentity(
          new UserIdentity().withAutoUser(new AutoUserSpecification().withElevationLevel(ElevationLevel.ADMIN)));
    }

    this.client.taskOperations().createTask(jobId, taskAddParameter);
  }

  /**
   * List the tasks of the specified job.
   *
   * @param jobId the ID of the job.
   * @return A list of CloudTask objects.
   */
  public List<CloudTask> getTaskStatusForJob(final String jobId) {
    List<CloudTask> tasks = null;
    try {
      tasks = client.taskOperations().listTasks(jobId);
      LOG.log(Level.INFO, "Task status for job: {0} returned {1} tasks", new Object[]{jobId, tasks.size()});
    } catch (IOException | BatchErrorException ex) {
      LOG.log(Level.SEVERE, "Exception when fetching Task status for job: {0}. Exception [{1}]:[2]",
          new Object[]{jobId, ex.getMessage(), ex.getStackTrace()});
    }

    return tasks;
  }

  /**
   * @return the job ID specified in the current system environment.
   */
  public String getAzureBatchJobId() {
    return System.getenv(AZ_BATCH_JOB_ID_ENV);
  }

  private TaskContainerSettings createTaskContainerSettings(final String dockerContainerId) {
    if (!this.areContainersEnabled) {
      return null;
    }

    final StringBuilder runOptions = new StringBuilder(String.format(
        "-d --rm --name %s --env %s=%s ",
        dockerContainerId,
        ContainerBasedLocalAddressProvider.HOST_IP_ADDR_PATH_ENV,
        this.commandBuilder.getIpAddressFilePath()));

    for (final int port : this.ports) {
      runOptions.append(String.format("-p %d:%d ", port, port));
    }

    return new TaskContainerSettings()
        .withRegistry(this.containerRegistryProvider.getContainerRegistry())
        .withImageName(this.containerRegistryProvider.getContainerImageName())
        .withContainerRunOptions(runOptions.toString());
  }

  private JobPreparationTask createJobPreparationTask() {
    if (!this.areContainersEnabled) {
      return null;
    }

    final String captureIpAddressCommandLine = this.commandBuilder.captureIpAddressCommandLine();
    return new JobPreparationTask()
        .withId(CAPTURE_HOST_IP_ADDRESS_TASK_NAME)
        .withCommandLine(captureIpAddressCommandLine);
  }

  public static Set<String> toStringSet(final Collection<?> items) {
    final Set<String> set = new HashSet<>(items.size());
    for (final Object elem: items) {
      set.add(elem.toString());
    }
    return Collections.unmodifiableSet(set);
  }
}
