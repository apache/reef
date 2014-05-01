package com.microsoft.reef.webserver;

import com.microsoft.reef.driver.catalog.NodeDescriptor;
import com.microsoft.reef.driver.context.ActiveContext;
import com.microsoft.reef.driver.context.ClosedContext;
import com.microsoft.reef.driver.context.FailedContext;
import com.microsoft.reef.driver.evaluator.AllocatedEvaluator;
import com.microsoft.reef.driver.evaluator.EvaluatorDescriptor;
import com.microsoft.reef.driver.evaluator.FailedEvaluator;
import com.microsoft.reef.driver.task.CompletedTask;
import com.microsoft.reef.driver.task.RunningTask;
import com.microsoft.tang.annotations.Unit;
import com.microsoft.wake.EventHandler;
import com.microsoft.wake.time.Time;
import com.microsoft.wake.time.event.StartTime;
import com.microsoft.wake.time.event.StopTime;

import javax.inject.Inject;
import java.net.InetSocketAddress;
import java.security.Timestamp;
import java.text.Format;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Vector;
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
    private Map<String, EvaluatorDescriptor> evaluators = new HashMap<>();

    /**
     * Evaluator start time
     */
    private StartTime startTime;

    /**
     * Evaluator stop time
     */
    private StopTime stopTime;

    /**
     *  ReefEventStateManager that keeps the states of Reef components
     */
    @Inject
    public ReefEventStateManager() {
    }

    /**
     * get start time
     * @return
     */
    public String getStartTime() {
        return convertTime(startTime.getTimeStamp());
    }

    /**
     * get stop time
     * @return
     */
    public String getStopTime() {
        return convertTime(stopTime.getTimeStamp());
    }

    /**
     * convert time from long to formatted string
     * @param time
     * @return
     */
    private String convertTime(long time){
        Date date = new Date(time);
        Format format = new SimpleDateFormat("yyyy MM dd HH:mm:ss");
        return format.format(date).toString();
    }

    /**
     * get evaluator map
     * @return
     */
    public Map<String, EvaluatorDescriptor> getEvaluators() {
        return evaluators;
    }

    /**
     * getEvaluatorDescriptor
     * @param evaluatorId
     * @return
     */
    public EvaluatorDescriptor getEvaluatorDescriptor(String evaluatorId) {
        return evaluators.get(evaluatorId);
    }

    /**
     * get Evaluator NodeDescriptor
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
                LOG.log(Level.INFO, "AllocatedEvaluatorStateHandler. EvalId: " + eval.getId());
                LOG.log(Level.INFO, "AllocatedEvaluatorStateHandler. NodeIp:" + eval.getEvaluatorDescriptor().getNodeDescriptor().getInetSocketAddress());
                LOG.log(Level.INFO, "AllocatedEvaluatorStateHandler. NodeName:" + eval.getEvaluatorDescriptor().getNodeDescriptor().getName());
                ReefEventStateManager.this.evaluators.put(eval.getId(), eval.getEvaluatorDescriptor());
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
                LOG.log(Level.INFO, "ClientMessageStateHandler"+  message.toString());
            }
        }
    }
}
