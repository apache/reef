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
package com.microsoft.reef.io.data.loading.api;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.inject.Inject;

import com.microsoft.reef.annotations.audience.DriverSide;
import com.microsoft.reef.driver.context.ContextConfiguration;
import com.microsoft.reef.driver.evaluator.AllocatedEvaluator;
import com.microsoft.reef.driver.evaluator.EvaluatorRequest;
import com.microsoft.reef.driver.evaluator.EvaluatorRequestor;
import com.microsoft.reef.driver.evaluator.FailedEvaluator;
import com.microsoft.reef.io.data.loading.impl.EvaluatorRequestSerializer;
import com.microsoft.reef.io.network.util.Utils.Pair;
import com.microsoft.tang.Configuration;
import com.microsoft.tang.annotations.Parameter;
import com.microsoft.tang.annotations.Unit;
import com.microsoft.tang.exceptions.BindException;
import com.microsoft.wake.EventHandler;
import com.microsoft.wake.impl.SingleThreadStage;
import com.microsoft.wake.time.Clock;
import com.microsoft.wake.time.event.Alarm;
import com.microsoft.wake.time.event.StartTime;


// TODO: Add timeouts
/**
 * The driver component for the DataLoadingService
 * Also acts as the central point for resource requests
 * All the allocated evaluators pass through this and
 * the ones that need data loading have a context stacked
 * that enables a task to get access to Data via the
 * {@link DataSet}
 */
@DriverSide
@Unit
public class DataLoader {
  /**
   * Standard Java logger object.
   */
  private static final Logger LOG = Logger.getLogger(DataLoader.class.getName());
  private final DataLoadingService dataLoadingService;
  private final int dataEvalMemoryMB;
  private final EvaluatorRequest computeRequest;
  private final AtomicInteger numComputeRequestsToSubmit;
  private final SingleThreadStage<EvaluatorRequest> resourceRequestStage;
  private final ResourceRequestHandler resourceRequestHandler;
  private final ConcurrentMap<String,Pair<Configuration, Configuration>> submittedDataEvalConfigs;
  private final ConcurrentMap<String,Configuration> submittedComputeEvalConfigs;
  private final int computeEvalMemoryMB;
  private final EvaluatorRequestor requestor;
  private final BlockingQueue<Configuration> failedComputeEvalConfigs;
  private final BlockingQueue<Pair<Configuration, Configuration>> failedDataEvalConfigs;


  @Inject
  public DataLoader(
      final Clock clock,
      final EvaluatorRequestor requestor,
      final DataLoadingService dataLoadingService,
      @Parameter(DataLoadingRequestBuilder.DataLoadingEvaluatorMemoryMB.class) final int dataEvalMemoryMB,
      @Parameter(DataLoadingRequestBuilder.DataLoadingComputeRequest.class) final String serializedComputeRequest
      ) {
    clock.scheduleAlarm(30000, new EventHandler<Alarm>() {

      @Override
      public void onNext(final Alarm arg0) {
        LOG.log(Level.FINE,"Received Alarm");
      }
    });
    this.requestor = requestor;
    this.submittedDataEvalConfigs = new ConcurrentHashMap<>();
    this.submittedComputeEvalConfigs = new ConcurrentHashMap<>();
    this.failedDataEvalConfigs = new LinkedBlockingQueue<>();
    this.failedComputeEvalConfigs = new LinkedBlockingQueue<>();
    this.dataLoadingService = dataLoadingService;
    this.dataEvalMemoryMB = dataEvalMemoryMB;
    this.resourceRequestHandler  = new ResourceRequestHandler(requestor);
    resourceRequestStage = new SingleThreadStage<>(resourceRequestHandler, 2);
    if(!serializedComputeRequest.equals("NULL")){
      this.computeRequest = EvaluatorRequestSerializer.deserialize(serializedComputeRequest);
      this.computeEvalMemoryMB = computeRequest.getMegaBytes();
      this.numComputeRequestsToSubmit = new AtomicInteger(computeRequest.getNumber());
      resourceRequestStage.onNext(computeRequest);
    }
    else{
      this.computeRequest = null;
      this.computeEvalMemoryMB = -1;
      this.numComputeRequestsToSubmit = new AtomicInteger(0);
    }
    resourceRequestStage.onNext(getDataLoadingRequest());
  }

  private EvaluatorRequest getDataLoadingRequest() {
    return EvaluatorRequest.newBuilder()
            .setNumber(dataLoadingService.getNumberOfPartitions())
            .setMemory(dataEvalMemoryMB)
            .build();
  }

  public class StartHandler implements EventHandler<StartTime> {

    @Override
    public void onNext(final StartTime startTime) {
      LOG.log(Level.INFO,"StartTime: " + startTime.toString());
      resourceRequestHandler.releaseResourceRequestGate();
    }
  }

  public class EvaluatorAllocatedHandler implements EventHandler<AllocatedEvaluator> {

    @Override
    public void onNext(final AllocatedEvaluator allocatedEvaluator) {
      LOG.log(Level.INFO,"Received an allocated evaluator.");
      if(!failedComputeEvalConfigs.isEmpty()){
        LOG.log(Level.INFO,"Failed Compute requests need to be satisfied.");
        if(allocatedEvaluator.getEvaluatorDescriptor().getMemory()==computeEvalMemoryMB){
          LOG.log(Level.INFO,"My resources match compute request resources.");
          final Configuration conf = failedComputeEvalConfigs.poll();
          if(conf!=null){
            LOG.log(Level.INFO,"Satisfying a failed configuration.");
            allocatedEvaluator.submitContext(conf);
            submittedComputeEvalConfigs.put(allocatedEvaluator.getId(),conf);
            return;
          }
        }
      }
      if(!failedDataEvalConfigs.isEmpty()){
        LOG.log(Level.INFO,"Failed Data requests need to be satisfied.");
        if(allocatedEvaluator.getEvaluatorDescriptor().getMemory()==dataEvalMemoryMB){
          LOG.log(Level.INFO,"My resources match data request resources.");
          final Pair<Configuration, Configuration> confPair = failedDataEvalConfigs.poll();
          if(confPair!=null){
            LOG.log(Level.INFO,"Satisfying a failed configuration.");
            allocatedEvaluator.submitContextAndService(confPair.first, confPair.second);
            submittedDataEvalConfigs.put(allocatedEvaluator.getId(), new Pair<>(confPair.first, confPair.second));
            return;
          }
        }
      }


      final int evaluatorsForComputeRequest = numComputeRequestsToSubmit.decrementAndGet();
      LOG.log(Level.FINE,"Evals For Compute Request: " + evaluatorsForComputeRequest);
      if(evaluatorsForComputeRequest >= 0){
        try {
          final Configuration idConfiguration = ContextConfiguration.CONF
              .set(ContextConfiguration.IDENTIFIER, dataLoadingService.getComputeContextIdPrefix() + evaluatorsForComputeRequest)
              .build();
          LOG.log(Level.FINE,"Submitting Compute Context");
          allocatedEvaluator.submitContext(idConfiguration);
          submittedComputeEvalConfigs.put(allocatedEvaluator.getId(),idConfiguration);
          if(evaluatorsForComputeRequest == 0){
            LOG.log(Level.FINE,"All Compute requests satisfied. Releasing gate");
            resourceRequestHandler.releaseResourceRequestGate();
          }
        } catch (final BindException e) {
          throw new RuntimeException("Unable to bind context id for Compute request", e);
        }

      }
      else{
        LOG.log(Level.FINE,"Getting evals for data loading");
        final Configuration contextConfiguration = dataLoadingService.getContextConfiguration(allocatedEvaluator);
        final Configuration serviceConfiguration = dataLoadingService.getServiceConfiguration(allocatedEvaluator);
        LOG.log(Level.FINE,"Submitting data loading context");
        allocatedEvaluator.submitContextAndService(contextConfiguration, serviceConfiguration);
        submittedDataEvalConfigs.put(allocatedEvaluator.getId(), new Pair<>(contextConfiguration, serviceConfiguration));
      }
    }
  }

  public class FailedEvaluatorHandler implements EventHandler<FailedEvaluator>{

    @Override
    public void onNext(final FailedEvaluator failedEvaluator) {
      if(submittedComputeEvalConfigs.containsKey(failedEvaluator.getId())){
        LOG.log(Level.INFO,"Received a failed compute evaluator.");
        failedComputeEvalConfigs.add(submittedComputeEvalConfigs.remove(failedEvaluator.getId()));
        requestor.submit(EvaluatorRequest.newBuilder().setMemory(computeEvalMemoryMB).setNumber(1).build());
      }
      else if(submittedDataEvalConfigs.containsKey(failedEvaluator.getId())){
        LOG.log(Level.INFO,"Received a failed data evaluator.");
        failedDataEvalConfigs.add(submittedDataEvalConfigs.remove(failedEvaluator.getId()));
        requestor.submit(EvaluatorRequest.newBuilder().setMemory(dataEvalMemoryMB).setNumber(1).build());
      }
      else{
        throw new RuntimeException("Received a failed evaluator that I did not submit");
      }

    }

  }

}
