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
package com.microsoft.reef.webserver;

import com.microsoft.reef.driver.catalog.NodeDescriptor;
import com.microsoft.reef.driver.context.ActiveContext;
import com.microsoft.reef.driver.evaluator.AllocatedEvaluator;
import com.microsoft.reef.driver.evaluator.EvaluatorDescriptor;
import com.microsoft.reef.driver.task.RunningTask;
import com.microsoft.tang.annotations.Unit;
import com.microsoft.wake.EventHandler;
import com.microsoft.wake.time.event.StartTime;
import com.microsoft.wake.time.event.StopTime;

import javax.inject.Inject;
import java.text.Format;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Reef Event Manager that manages Reef states
 */
@Unit
public final class ReefEventStateManager {
    /**
     * Standard Java logger.
     */
    private static final Logger LOG = Logger.getLogger(ReefEventStateManager.class.getName());

    /**
     * Map of evaluators
     */
    private final Map<String, EvaluatorDescriptor> evaluators = new HashMap<>();

    /**
     * Evaluator start time
     */
    private StartTime startTime;

    /**
     * Evaluator stop time
     */
    private StopTime stopTime;

    /**
     * ReefEventStateManager that keeps the states of Reef components
     */
    @Inject
    public ReefEventStateManager() {
    }

    /**
     * get start time
     *
     * @return
     */
    public String getStartTime() {
        return convertTime(startTime.getTimeStamp());
    }

    /**
     * get stop time
     *
     * @return
     */
    public String getStopTime() {
        return convertTime(stopTime.getTimeStamp());
    }

    /**
     * convert time from long to formatted string
     *
     * @param time
     * @return
     */
    private String convertTime(long time) {
        Date date = new Date(time);
        Format format = new SimpleDateFormat("yyyy MM dd HH:mm:ss");
        return format.format(date).toString();
    }

    /**
     * get evaluator map
     *
     * @return
     */
    public Map<String, EvaluatorDescriptor> getEvaluators() {
        return evaluators;
    }

    /**
     * pus a entry to evaluators
     *
     * @param key
     * @param value
     */
    public void put(String key, EvaluatorDescriptor value) {
        evaluators.put(key, value);
    }

    /**
     * get a value from evaluators by key
     *
     * @param key
     * @return
     */
    public EvaluatorDescriptor get(String key) {
        return evaluators.get(key);
    }

    /**
     * getEvaluatorDescriptor
     *
     * @param evaluatorId
     * @return
     */
    public EvaluatorDescriptor getEvaluatorDescriptor(String evaluatorId) {
        return evaluators.get(evaluatorId);
    }

    /**
     * get Evaluator NodeDescriptor
     *
     * @param evaluatorId
     * @return
     */
    public NodeDescriptor getEvaluatorNodeDescriptor(String evaluatorId) {
        return evaluators.get(evaluatorId).getNodeDescriptor();
    }

    /**
     * Job Driver is ready and the clock is set up: request the evaluators.
     */
    public final class StartStateHandler implements EventHandler<StartTime> {
        @Override
        public void onNext(final StartTime startTime) {
            LOG.log(Level.INFO, "StartStateHandler called. StartTime:" + startTime);
            ReefEventStateManager.this.startTime = startTime;
        }
    }

    /**
     * Shutting down the job driver: close the evaluators.
     */
    public final class StopStateHandler implements EventHandler<StopTime> {
        @Override
        public void onNext(final StopTime stopTime) {
            LOG.log(Level.INFO, "StopStateHandler called. StopTime:" + stopTime);
            ReefEventStateManager.this.stopTime = stopTime;
        }
    }

    /**
     * Receive notification that an Evaluator had been allocated,
     */
    public final class AllocatedEvaluatorStateHandler implements EventHandler<AllocatedEvaluator> {
        @Override
        public void onNext(final AllocatedEvaluator eval) {
            synchronized (ReefEventStateManager.this) {
                ReefEventStateManager.this.put(eval.getId(), eval.getEvaluatorDescriptor());
            }
        }
    }

    /**
     * Receive event when task is running
     */
    public final class TaskRunningStateHandler implements EventHandler<RunningTask> {
        @Override
        public void onNext(final RunningTask runningActivity) {
            String ip = runningActivity.getActiveContext().getEvaluatorDescriptor().getNodeDescriptor().getInetSocketAddress().toString();
            String id = runningActivity.getActiveContext().getId();
            LOG.log(Level.INFO, "RunningActivity on address: " + ip + " for id " + id);
        }
    }

    /**
     * Receive notification that a new Context is available.
     */
    public final class ActiveContextStateHandler implements EventHandler<ActiveContext> {
        @Override
        public void onNext(final ActiveContext context) {
            synchronized (ReefEventStateManager.this) {
                LOG.log(Level.INFO, "ActiveContextStateHandler called." + context.toString());
            }
        }
    }

    /**
     * Receive notification from the client.
     */
    public final class ClientMessageStateHandler implements EventHandler<byte[]> {
        @Override
        public void onNext(final byte[] message) {
            synchronized (ReefEventStateManager.this) {
                LOG.log(Level.INFO, "ClientMessageStateHandler" + message.toString());
            }
        }
    }
}
