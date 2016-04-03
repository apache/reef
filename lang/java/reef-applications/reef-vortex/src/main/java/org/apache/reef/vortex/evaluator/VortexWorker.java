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
package org.apache.reef.vortex.evaluator;

import org.apache.reef.annotations.Unstable;
import org.apache.reef.annotations.audience.TaskSide;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.annotations.Unit;
import org.apache.reef.task.HeartBeatTriggerManager;
import org.apache.reef.task.Task;
import org.apache.reef.task.TaskMessage;
import org.apache.reef.task.TaskMessageSource;
import org.apache.reef.task.events.CloseEvent;
import org.apache.reef.task.events.DriverMessage;
import org.apache.reef.util.Optional;
import org.apache.reef.vortex.common.KryoUtils;
import org.apache.reef.vortex.common.AggregateFunctionRepository;
import org.apache.reef.vortex.protocol.mastertoworker.*;
import org.apache.reef.vortex.protocol.workertomaster.*;
import org.apache.reef.vortex.driver.VortexWorkerConf;
import org.apache.reef.wake.EventHandler;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Receives commands from VortexMaster, executes them, and returns the results.
 * TODO[REEF-503]: Basic Vortex profiling.
 */
@Unstable
@Unit
@TaskSide
public final class VortexWorker implements Task, TaskMessageSource {
  private static final Logger LOG = Logger.getLogger(VortexWorker.class.getName());
  private static final String MESSAGE_SOURCE_ID = ""; // empty string as there is no use for it

  private final BlockingDeque<byte[]> pendingRequests = new LinkedBlockingDeque<>();
  private final BlockingDeque<byte[]> workerReports = new LinkedBlockingDeque<>();
  private final ConcurrentMap<Integer, AggregateContainer> aggregates = new ConcurrentHashMap<>();

  private final AggregateFunctionRepository aggregateFunctionRepository;
  private final KryoUtils kryoUtils;
  private final HeartBeatTriggerManager heartBeatTriggerManager;
  private final int numOfThreads;
  private final CountDownLatch terminated = new CountDownLatch(1);

  @Inject
  private VortexWorker(final HeartBeatTriggerManager heartBeatTriggerManager,
                       final AggregateFunctionRepository aggregateFunctionRepository,
                       final KryoUtils kryoUtils,
                       @Parameter(VortexWorkerConf.NumOfThreads.class) final int numOfThreads) {
    this.heartBeatTriggerManager = heartBeatTriggerManager;
    this.aggregateFunctionRepository = aggregateFunctionRepository;
    this.kryoUtils = kryoUtils;
    this.numOfThreads = numOfThreads;
  }

  /**
   * Starts the scheduler and executor and waits until termination.
   */
  @Override
  public byte[] call(final byte[] memento) throws Exception {
    final ExecutorService schedulerThread = Executors.newSingleThreadExecutor();
    final ExecutorService commandExecutor = Executors.newFixedThreadPool(numOfThreads);
    final ConcurrentMap<Integer, Future> futures = new ConcurrentHashMap<>();

    // Scheduling thread starts
    schedulerThread.execute(new Runnable() {
      @SuppressWarnings("InfiniteLoopStatement") // Scheduler is supposed to run forever.
      @Override
      public void run() {
        while (true) {
          // Scheduler Thread: Pick a command to execute (For now, simple FIFO order)
          final byte[] message;
          try {
            message = pendingRequests.takeFirst();
          } catch (InterruptedException e) {
            throw new RuntimeException(e);
          }

          // Command Executor: Deserialize the command
          final MasterToWorker masterToWorker = (MasterToWorker)kryoUtils.deserialize(message);

          switch (masterToWorker.getType()) {
            case AggregateTasklets:
              final TaskletAggregation taskletAggregation = (TaskletAggregation) masterToWorker;
              aggregates.put(taskletAggregation.getAggregateFunctionId(),
                  new AggregateContainer(heartBeatTriggerManager, kryoUtils, workerReports,
                      taskletAggregation));
              aggregateFunctionRepository.put(taskletAggregation.getAggregateFunctionId(),
                  taskletAggregation.getAggregateFunction(), taskletAggregation.getFunction(),
                  taskletAggregation.getPolicy());
              break;
            case ExecuteAggregateTasklet:
              executeAggregateTasklet(commandExecutor, masterToWorker);
              break;
            case ExecuteTasklet:
              executeTasklet(commandExecutor, futures, masterToWorker);
              break;
            case CancelTasklet:
              final TaskletCancellation cancellationRequest = (TaskletCancellation) masterToWorker;
              LOG.log(Level.FINE, "Cancelling Tasklet with ID {0}.", cancellationRequest.getTaskletId());
              final Future future = futures.get(cancellationRequest.getTaskletId());
              if (future != null) {
                future.cancel(true);
              }
              break;
            default:
              throw new RuntimeException("Unknown Command");
          }
        }
      }
    });

    terminated.await();
    return null;
  }

  /**
   * Executes an tasklet request from the {@link org.apache.reef.vortex.driver.VortexDriver}.
   */
  private void executeTasklet(final ExecutorService commandExecutor,
                              final ConcurrentMap<Integer, Future> futures,
                              final MasterToWorker masterToWorker) {
    final CountDownLatch latch = new CountDownLatch(1);
    final TaskletExecution taskletExecution = (TaskletExecution) masterToWorker;

    // Scheduler Thread: Pass the command to the worker thread pool to be executed
    // Record future to support cancellation.
    futures.put(
        taskletExecution.getTaskletId(),
        commandExecutor.submit(new Runnable() {
          @Override
          public void run() {
            final WorkerReport workerReport;
            final List<WorkerToMaster> workerToMasters = new ArrayList<>();

            try {
              // Command Executor: Execute the command
              final WorkerToMaster workerToMaster =
                  new TaskletResult(taskletExecution.getTaskletId(), taskletExecution.execute());
              workerToMasters.add(workerToMaster);
            } catch (final InterruptedException ex) {
              // Assumes that user's thread follows convention that cancelled Futures
              // should throw InterruptedException.
              final WorkerToMaster workerToMaster = new TaskletCancelled(taskletExecution.getTaskletId());
              LOG.log(Level.WARNING, "Tasklet with ID {0} has been cancelled", taskletExecution.getTaskletId());
              workerToMasters.add(workerToMaster);
            } catch (Exception e) {
              // Command Executor: Tasklet throws an exception
              final WorkerToMaster workerToMaster = new TaskletFailure(taskletExecution.getTaskletId(), e);
              workerToMasters.add(workerToMaster);
            }

            workerReport = new WorkerReport(workerToMasters);
            workerReports.addLast(kryoUtils.serialize(workerReport));
            try {
              latch.await();
            } catch (final InterruptedException e) {
              LOG.log(Level.SEVERE, "Cannot wait for Future to be put.");
              throw new RuntimeException(e);
            }
            futures.remove(taskletExecution.getTaskletId());
            heartBeatTriggerManager.triggerHeartBeat();
          }
        }));

    // Signal that future is put.
    latch.countDown();
  }

  /**
   * Executes an aggregation request from the {@link org.apache.reef.vortex.driver.VortexDriver}.
   */
  private void executeAggregateTasklet(final ExecutorService commandExecutor,
                                       final MasterToWorker masterToWorker) {
    final TaskletAggregateExecution taskletAggregateExecution = (TaskletAggregateExecution) masterToWorker;

    assert aggregates.containsKey(taskletAggregateExecution.getAggregateFunctionId());

    final AggregateContainer aggregateContainer = aggregates.get(taskletAggregateExecution.getAggregateFunctionId());
    final TaskletAggregation aggregationRequest = aggregateContainer.getTaskletAggregation();

    commandExecutor.submit(new Runnable() {
      @Override
      public void run() {
        try {
          aggregateContainer.scheduleTasklet(taskletAggregateExecution.getTaskletId());
          final Object result = aggregationRequest.executeFunction(taskletAggregateExecution.getInput());
          aggregateContainer.taskletComplete(taskletAggregateExecution.getTaskletId(), result);
        } catch (final Exception e) {
          aggregateContainer.taskletFailed(taskletAggregateExecution.getTaskletId(), e);
        }
      }
    });
  }

  /**
   * @return the workerReport the worker wishes to send.
   */
  @Override
  public Optional<TaskMessage> getMessage() {
    final byte[] msg = workerReports.pollFirst();
    if (msg != null) {
      return Optional.of(TaskMessage.from(MESSAGE_SOURCE_ID, msg));
    } else {
      return Optional.empty();
    }
  }

  /**
   * Handle requests from Vortex Master.
   */
  public final class DriverMessageHandler implements EventHandler<DriverMessage> {
    @Override
    public void onNext(final DriverMessage message) {
      if (message.get().isPresent()) {
        pendingRequests.addLast(message.get().get());
      }
    }
  }

  /**
   * Shut down this worker.
   */
  public final class TaskCloseHandler implements EventHandler<CloseEvent> {
    @Override
    public void onNext(final CloseEvent closeEvent) {
      terminated.countDown();
    }
  }
}
