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
package org.apache.reef.runtime.spark.evaluator;

import com.google.protobuf.ByteString;
import org.apache.reef.runtime.common.files.REEFFileNames;
import org.apache.reef.runtime.spark.evaluator.parameters.SparkExecutorId;
import org.apache.reef.runtime.spark.util.EvaluatorControl;
import org.apache.reef.runtime.spark.util.EvaluatorLaunch;
import org.apache.reef.runtime.spark.util.EvaluatorRelease;
import org.apache.reef.runtime.spark.util.SparkRemoteManager;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.JavaConfigurationBuilder;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.formats.CommandLine;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.spark.Executor;
import org.apache.spark.ExecutorDriver;
import org.apache.spark.SparkExecutorDriver;

import javax.inject.Inject;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * REEF implementation of Spark Executor.
 */
public final class REEFExecutor implements Executor {
  private static final Logger LOG = Logger.getLogger(REEFExecutor.class.getName());

  private final SparkExecutorDriver sparkExecutorDriver;
  private final SparkRemoteManager sparkRemoteManager;
  private final ExecutorService executorService;
  private final REEFFileNames fileNames;
  private final String mesosExecutorId;

  private Process evaluatorProcess;
  private Integer evaluatorProcessExitValue;

  @Inject
  REEFExecutor(final EvaluatorControlHandler evaluatorControlHandler,
               final SparkRemoteManager sparkRemoteManager,
               final REEFFileNames fileNames,
               @Parameter(SparkExecutorId.class) final String mesosExecutorId) {
    this.sparkRemoteManager = sparkRemoteManager;
    this.sparkRemoteManager.registerHandler(EvaluatorControl.class, evaluatorControlHandler);
    this.sparkExecutorDriver = new SparkExecutorDriver(this);
    this.executorService = Executors.newCachedThreadPool();
    this.fileNames = fileNames;
    this.mesosExecutorId = mesosExecutorId;
  }

  @Override
  public void registered(final ExecutorDriver driver,
                         final ExecutorInfo executorInfo,
                         final FrameworkInfo frameworkInfo,
                         final SlaveInfo slaveInfo) {
    LOG.log(Level.FINEST, "Executor registered. driver: {0} executorInfo: {1} frameworkInfo: {2} slaveInfo {3}",
        new Object[]{driver, executorInfo, frameworkInfo, slaveInfo});
  }

  @Override
  public void reregistered(final ExecutorDriver driver, final SlaveInfo slaveInfo) {
    LOG.log(Level.FINEST, "Executor reregistered. driver: {0}", driver);
  }

  @Override
  public void disconnected(final ExecutorDriver driver) {
    this.onRuntimeError();
  }

  /**
   * We assume a long-running Spark Task that manages a REEF Evaluator process, leveraging Spark Executor's interface.
   */
  @Override
  public void launchTask(final ExecutorDriver driver, final TaskInfo task) {
    driver.sendStatusUpdate(TaskStatus.newBuilder()
        .setTaskId(TaskID.newBuilder().setValue(this.mesosExecutorId).build())
        .setState(TaskState.TASK_STARTING)
        .setSlaveId(task.getSlaveId())
        .setMessage(this.sparkRemoteManager.getMyIdentifier())
        .build());
  }

  @Override
  public void killTask(final ExecutorDriver driver, final TaskID taskId) {
    this.onStop();
  }

  @Override
  public void frameworkMessage(final ExecutorDriver driver, final byte[] data) {
    LOG.log(Level.FINEST, "Framework Messge. ExecutorDriver: {0}, data: {1}.",
        new Object[]{driver, data});
  }

  @Override
  public void shutdown(final ExecutorDriver driver) {
    this.onStop();
  }

  @Override
  public void error(final ExecutorDriver driver, final String message) {
    this.onRuntimeError();
  }

  /////////////////////////////////////////////////////////////////
  // HELPER METHODS

  private void onStart() {
    this.executorService.submit(new Thread() {
      public void run() {
        final Status status;
        status = mesosExecutorDriver.run();
        LOG.log(Level.INFO, "MesosExecutorDriver ended with status {0}", status);
      }
    });
  }

  private void onStop() {
    // Shutdown REEF Evaluator
    if (this.evaluatorProcess != null) {
      this.evaluatorProcess.destroy();
      sparkExecutorDriver.sendStatusUpdate(TaskStatus.newBuilder()
          .setTaskId(TaskID.newBuilder()
              .setValue(mesosExecutorId)
              .build())
          .setState(TaskState.TASK_FINISHED)
          .setMessage("Evaluator Process exited with status " + String.valueOf(evaluatorProcessExitValue))
          .build());
    } else {
      mesosExecutorDriver.sendStatusUpdate(TaskStatus.newBuilder()
          .setTaskId(TaskID.newBuilder()
              .setValue(mesosExecutorId)
              .build())
          .setState(TaskState.TASK_FINISHED)
          .setData(ByteString.copyFromUtf8("eval_not_run"))
          // TODO[JIRA REEF-102]: a hack to pass closeEvaluator test, replace this with a better interface
          .setMessage("Evaluator Process exited with status " + String.valueOf(evaluatorProcessExitValue))
          .build());
    }

    // Shutdown Mesos Executor
    this.executorService.shutdown();
    this.sparkExecutorDriver.stop();
  }

  private void onRuntimeError() {
    // Shutdown REEF Evaluator
    if (this.evaluatorProcess != null) {
      this.evaluatorProcess.destroy();
    }
    sparkExecutorDriver.sendStatusUpdate(TaskStatus.newBuilder()
        .setTaskId(TaskID.newBuilder()
            .setValue(mesosExecutorId)
            .build())
        .setState(TaskState.TASK_FAILED)
        .setMessage("Evaluator Process exited with status " + String.valueOf(evaluatorProcessExitValue))
        .build());

    // Shutdown Mesos Executor
    this.executorService.shutdown();
    this.sparkExecutorDriver.stop();
  }

  public void onEvaluatorRelease(final EvaluatorRelease evaluatorRelease) {
    LOG.log(Level.INFO, "Release!!!! {0}", evaluatorRelease.toString());
    assert evaluatorRelease.getIdentifier().toString().equals(this.mesosExecutorId);
    this.onStop();
  }

  public void onEvaluatorLaunch(final EvaluatorLaunch evaluatorLaunch) {
    LOG.log(Level.INFO, "Launch!!!! {0}", evaluatorLaunch.toString());
    assert evaluatorLaunch.getIdentifier().toString().equals(this.mesosExecutorId);
    final ExecutorService evaluatorLaunchExecutorService = Executors.newSingleThreadExecutor();
    evaluatorLaunchExecutorService.submit(new Thread() {
      public void run() {
        try {
          final List<String> command = Arrays.asList(evaluatorLaunch.getCommand().toString().split(" "));
          LOG.log(Level.INFO, "Command!!!! {0}", command);
          final FileSystem fileSystem = FileSystem.get(new Configuration());
          final Path hdfsFolder = new Path(fileSystem.getUri() + "/" + mesosExecutorId);
          final File localFolder = new File(fileNames.getREEFFolderName(), fileNames.getLocalFolderName());

          FileUtil.copy(fileSystem, hdfsFolder, localFolder, true, new Configuration());

          evaluatorProcess = new ProcessBuilder()
              .command(command)
              .redirectError(new File(fileNames.getEvaluatorStderrFileName()))
              .redirectOutput(new File(fileNames.getEvaluatorStdoutFileName()))
              .start();

          evaluatorProcessExitValue = evaluatorProcess.waitFor();

          fileSystem.close();
        } catch (IOException | InterruptedException e) {
          throw new RuntimeException(e);
        }
      }
    });
    evaluatorLaunchExecutorService.shutdown();
  }

  public static org.apache.reef.tang.Configuration parseCommandLine(final String[] args) throws IOException {
    final JavaConfigurationBuilder confBuilder = Tang.Factory.getTang().newConfigurationBuilder();

    new CommandLine(confBuilder)
        .registerShortNameOfClass(SparkExecutorId.class)
        .processCommandLine(args);

    return confBuilder.build();
  }

  /**
   * The starting point of the executor.
   */
  public static void main(final String[] args) throws Exception {
    final Injector injector = Tang.Factory.getTang().newInjector(parseCommandLine(args));
    final REEFExecutor reefExecutor = injector.getInstance(REEFExecutor.class);
    reefExecutor.onStart();
  }
}
