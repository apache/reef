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
package org.apache.reef.io.data.loading.api;

import org.apache.commons.lang.Validate;
import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.driver.context.ContextConfiguration;
import org.apache.reef.driver.evaluator.AllocatedEvaluator;
import org.apache.reef.driver.evaluator.EvaluatorRequest;
import org.apache.reef.driver.evaluator.EvaluatorRequestor;
import org.apache.reef.driver.evaluator.FailedEvaluator;
import org.apache.reef.io.data.loading.impl.AvroEvaluatorRequestSerializer;
import org.apache.reef.io.network.util.Pair;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.annotations.Unit;
import org.apache.reef.tang.exceptions.BindException;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.impl.SingleThreadStage;
import org.apache.reef.wake.time.Clock;
import org.apache.reef.wake.time.event.Alarm;
import org.apache.reef.wake.time.event.StartTime;

import javax.inject.Inject;

import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * The driver component for the DataLoadingService
 * Also acts as the central point for resource requests
 * All the allocated evaluators pass through this and
 * the ones that need data loading have a context stacked
 * that enables a task to get access to Data via the
 * {@link DataSet}.
 * <p/>
 * TODO: Add timeouts
 */
@DriverSide
@Unit
public class DataLoader {

  private static final Logger LOG = Logger.getLogger(DataLoader.class.getName());

  private final ConcurrentMap<String, Pair<Configuration, Configuration>> submittedDataEvalConfigs =
      new ConcurrentHashMap<>();
  private final ConcurrentMap<String, Configuration> submittedComputeEvalConfigs = new ConcurrentHashMap<>();
  private final BlockingQueue<Configuration> failedComputeEvalConfigs = new LinkedBlockingQueue<>();
  private final BlockingQueue<Pair<Configuration, Configuration>> failedDataEvalConfigs = new LinkedBlockingQueue<>();

  private final AtomicInteger numComputeRequestsToSubmit = new AtomicInteger(0);
  private final AtomicInteger numDataRequestsToSubmit = new AtomicInteger(0);

  private final DataLoadingService dataLoadingService;
  private int dataEvalMemoryMB;
  private int dataEvalCore;
  private final SingleThreadStage<EvaluatorRequest> resourceRequestStage;
  private final ResourceRequestHandler resourceRequestHandler;
  private int computeEvalMemoryMB;
  private int computeEvalCore;
  private final EvaluatorRequestor requestor;

  /**
   * Allows to specify compute and data evaluator requests in particular
   * locations.
   *
   * @param clock
   *          the clock
   * @param requestor
   *          the evaluator requestor
   * @param dataLoadingService
   *          the data loading service
   * @param serializedComputeRequests
   *          serialized compute requests (evaluators that will not load data)
   * @param serializedDataRequests
   *          serialized data requests (evaluators that will load data). It
   *          cannot be empty (to maintain previous functionality)
   */
  @Inject
  public DataLoader(
      final Clock clock,
      final EvaluatorRequestor requestor,
      final DataLoadingService dataLoadingService,
      @Parameter(DataLoadingRequestBuilder.DataLoadingComputeRequests.class)
      final Set<String> serializedComputeRequests,
      @Parameter(DataLoadingRequestBuilder.DataLoadingDataRequests.class) final Set<String> serializedDataRequests) {
    // data requests should not be empty. This maintains previous functionality
    Validate.notEmpty(serializedDataRequests, "Should contain a data request object");
    // FIXME: Issue #855: We need this alarm to look busy for REEF.
    clock.scheduleAlarm(30000, new EventHandler<Alarm>() {
      @Override
      public void onNext(final Alarm time) {
        LOG.log(Level.FINE, "Received Alarm: {0}", time);
      }
    });

    this.requestor = requestor;
    this.dataLoadingService = dataLoadingService;
    this.resourceRequestHandler = new ResourceRequestHandler(requestor);
    // the resource request queue will have as many requests as compute and data requests.
    this.resourceRequestStage = new SingleThreadStage<>(
        this.resourceRequestHandler, serializedComputeRequests.size()
            + serializedDataRequests.size());

    if (serializedComputeRequests.isEmpty()) {
      this.computeEvalMemoryMB = -1;
      this.computeEvalCore = 1;
    } else {
      // Deserialize each compute request.
      // Keep the maximum number of cores and memory requested, in case some
      // evaluator fails, we will try to reallocate based on that.
      for (final String serializedComputeRequest : serializedComputeRequests) {
        final EvaluatorRequest computeRequest = AvroEvaluatorRequestSerializer.fromString(serializedComputeRequest);
        this.numComputeRequestsToSubmit.addAndGet(computeRequest.getNumber());
        this.computeEvalMemoryMB = Math.max(this.computeEvalMemoryMB, computeRequest.getMegaBytes());
        this.computeEvalCore = Math.max(this.computeEvalCore, computeRequest.getNumberOfCores());
        this.resourceRequestStage.onNext(computeRequest);
      }
    }
    // Deserialize each data requests.
    // We distribute the partitions evenly across the DCs.
    // The number of partitions extracted from the dataLoadingService override
    // the number of evaluators requested (this preserves previous functionality)
    final int dcs = serializedDataRequests.size();
    final int partitionsPerDataCenter = this.dataLoadingService.getNumberOfPartitions() / dcs;
    int missing = this.dataLoadingService.getNumberOfPartitions() % dcs;
    for (final String serializedDataRequest : serializedDataRequests) {
      EvaluatorRequest dataRequest = AvroEvaluatorRequestSerializer.fromString(serializedDataRequest);
      this.dataEvalMemoryMB = Math.max(this.dataEvalMemoryMB, dataRequest.getMegaBytes());
      this.dataEvalCore = Math.max(this.dataEvalCore, dataRequest.getNumberOfCores());
      // clone the request but update the number of evaluators based on the number of partitions
      int number = partitionsPerDataCenter;
      if (missing > 0) {
        number++;
        missing--;
      }
      dataRequest = EvaluatorRequest.newBuilder(dataRequest).setNumber(number).build();
      this.numDataRequestsToSubmit.addAndGet(number);
      this.resourceRequestStage.onNext(dataRequest);
    }
  }

  public class StartHandler implements EventHandler<StartTime> {
    @Override
    public void onNext(final StartTime startTime) {
      LOG.log(Level.INFO, "StartTime: {0}", startTime);
      resourceRequestHandler.releaseResourceRequestGate();
    }
  }

  public class EvaluatorAllocatedHandler implements EventHandler<AllocatedEvaluator> {

    @Override
    public void onNext(final AllocatedEvaluator allocatedEvaluator) {

      final String evalId = allocatedEvaluator.getId();
      LOG.log(Level.FINEST, "Allocated evaluator: {0}", evalId);

      if (!failedComputeEvalConfigs.isEmpty()) {
        LOG.log(Level.FINE, "Failed Compute requests need to be satisfied for {0}", evalId);
        final Configuration conf = failedComputeEvalConfigs.poll();
        if (conf != null) {
          LOG.log(Level.FINE, "Satisfying failed configuration for {0}", evalId);
          allocatedEvaluator.submitContext(conf);
          submittedComputeEvalConfigs.put(evalId, conf);
          return;
        }
      }

      if (!failedDataEvalConfigs.isEmpty()) {
        LOG.log(Level.FINE, "Failed Data requests need to be satisfied for {0}", evalId);
        final Pair<Configuration, Configuration> confPair = failedDataEvalConfigs.poll();
        if (confPair != null) {
          LOG.log(Level.FINE, "Satisfying failed configuration for {0}", evalId);
          allocatedEvaluator.submitContextAndService(confPair.getFirst(), confPair.getSecond());
          submittedDataEvalConfigs.put(evalId, confPair);
          return;
        }
      }

      final int evaluatorsForComputeRequest = numComputeRequestsToSubmit.decrementAndGet();

      if (evaluatorsForComputeRequest >= 0) {
        LOG.log(Level.FINE, "Evaluators for compute request: {0}", evaluatorsForComputeRequest);
        try {
          final Configuration idConfiguration = ContextConfiguration.CONF.set(
              ContextConfiguration.IDENTIFIER,
              dataLoadingService.getComputeContextIdPrefix()
                  + evaluatorsForComputeRequest).build();
          LOG.log(Level.FINE, "Submitting Compute Context to {0}", evalId);
          allocatedEvaluator.submitContext(idConfiguration);
          submittedComputeEvalConfigs.put(allocatedEvaluator.getId(),
              idConfiguration);
          // should release the request gate when there are >= 0 compute
          // requests (now that we can have more than 1)
          LOG.log(
              Level.FINE,
              evaluatorsForComputeRequest > 0 ? "More Compute requests need to be satisfied"
                  : "All Compute requests satisfied." + " Releasing gate");
          resourceRequestHandler.releaseResourceRequestGate();
        } catch (final BindException e) {
          throw new RuntimeException(
              "Unable to bind context id for Compute request", e);
        }

      } else {

        final int evaluatorsForDataRequest = numDataRequestsToSubmit.decrementAndGet();
        LOG.log(Level.FINE, "Evaluators for data request: {0}", evaluatorsForDataRequest);

        final Pair<Configuration, Configuration> confPair = new Pair<>(
            dataLoadingService.getContextConfiguration(allocatedEvaluator),
            dataLoadingService.getServiceConfiguration(allocatedEvaluator));

        LOG.log(Level.FINE, "Submitting data loading context to {0}", evalId);
        allocatedEvaluator.submitContextAndService(confPair.getFirst(), confPair.getSecond());
        submittedDataEvalConfigs.put(allocatedEvaluator.getId(), confPair);

        // release the gate to keep on asking for more "data" evaluators.
        if (evaluatorsForDataRequest > 0) {
          LOG.log(Level.FINE, "More Data requests need to be satisfied. Releasing gate");
          resourceRequestHandler.releaseResourceRequestGate();
        // don't need to release if it's 0
        } else if (evaluatorsForDataRequest == 0) {
          LOG.log(Level.FINE, "All Data requests satisfied");
        }
      }
    }
  }

  public class EvaluatorFailedHandler implements EventHandler<FailedEvaluator> {
    @Override
    public void onNext(final FailedEvaluator failedEvaluator) {

      final String evalId = failedEvaluator.getId();

      final Configuration computeConfig = submittedComputeEvalConfigs.remove(evalId);
      if (computeConfig != null) {

        LOG.log(Level.INFO, "Received failed compute evaluator: {0}", evalId);
        failedComputeEvalConfigs.add(computeConfig);

        requestor.submit(EvaluatorRequest.newBuilder()
            .setMemory(computeEvalMemoryMB).setNumber(1).setNumberOfCores(computeEvalCore).build());

      } else {

        final Pair<Configuration, Configuration> confPair = submittedDataEvalConfigs.remove(evalId);
        if (confPair != null) {

          LOG.log(Level.INFO, "Received failed data evaluator: {0}", evalId);
          failedDataEvalConfigs.add(confPair);

          requestor.submit(EvaluatorRequest.newBuilder()
              .setMemory(dataEvalMemoryMB).setNumber(1).setNumberOfCores(dataEvalCore).build());

        } else {

          LOG.log(Level.SEVERE, "Received unknown failed evaluator " + evalId,
              failedEvaluator.getEvaluatorException());

          throw new RuntimeException("Received failed evaluator that I did not submit: " + evalId);
        }
      }
    }
  }
}
