package com.microsoft.wake.impl;

import java.util.concurrent.ForkJoinTask;
import java.util.logging.Logger;

import javax.inject.Inject;

import com.microsoft.tang.annotations.Parameter;
import com.microsoft.wake.AbstractEStage;
import com.microsoft.wake.EventHandler;
import com.microsoft.wake.StageConfiguration;

/**
 * This Wake event handling stage uses a {@link ForkJoinPool}
 * to submit tasks. The advantage is that underlying workers
 * have separate queues instead of sharing one. The queues are load
 * balanced with work stealing.
 * 
 * The pool is provided to the constructor, so multiple stages
 * may use the same pool.
 * 
 * Some advantage in throughput over other stage implementations should be seen
 * when one wake stage is submitting to another using the same
 * {@link WakeSharedPool}. In this case, the new event may be executed
 * directly by that thread.
 * 
 * @param <T> type of events
 */
public class ForkPoolStage<T> extends AbstractEStage<T> {
  private static final Logger LOG = Logger.getLogger(ForkPoolStage.class.getName());
  
  private final EventHandler<T> handler;
  private final WakeSharedPool pool;
  
  @Inject
  public ForkPoolStage(@Parameter(StageConfiguration.StageName.class) String stageName,
                         @Parameter(StageConfiguration.StageHandler.class) EventHandler<T> handler,
                          WakeSharedPool sharedPool
                          ) {
    super(stageName);
    this.pool = sharedPool;
    this.handler = handler;
    //TODO: should WakeSharedPool register its stages?
    
    StageManager.instance().register(this);
  }
  
  @Inject
  public ForkPoolStage(@Parameter(StageConfiguration.StageHandler.class) EventHandler<T> handler,
                          WakeSharedPool sharedPool) {
    this(ForkPoolStage.class.getName(), handler, sharedPool);
  }
  
  @Override
  public void onNext(final T value) {
    beforeOnNext();
    pool.submit(new ForkJoinTask<T>() {
      @Override
      public T getRawResult() {
        // tasks have no results because they are events
        // this may be used for extensions
        return null;
      }
      @Override
      protected void setRawResult(T value) {
        // tasks have no results because they are events
        // this may be used for extensions
      }

      @Override
      protected boolean exec() {
        handler.onNext(value);
        afterOnNext();
        return true;
      }
    });
  }


  @Override
  public void close() throws Exception {
    LOG.warning("close(): "+pool.getClass().getName()+" "+pool+" must really be close()'d");
  }
  
}