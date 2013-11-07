package com.microsoft.reef.io.grouper.impl;
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

import com.microsoft.reef.io.Tuple;
import com.microsoft.reef.io.grouper.Grouper;
import com.microsoft.reef.io.grouper.GrouperEvent;
import com.microsoft.tang.annotations.Parameter;
import com.microsoft.wake.EStage;
import com.microsoft.wake.EventHandler;
import com.microsoft.wake.StageConfiguration;
import com.microsoft.wake.rx.Observer;
import org.apache.commons.lang.NotImplementedException;

import javax.inject.Inject;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.logging.Level;
import java.util.logging.Logger;

//TODO: document Comparable requirement on K for the skip list implementation
public class CombiningSnowshovelGrouper<InType, OutType, K, V> implements Grouper<InType> {
  private Logger LOG = Logger.getLogger(CombiningSnowshovelGrouper.class.getName());
  
  private ConcurrentSkipListMap<K, V> register;
  private boolean inputDone;
  private Combiner<OutType,K, V> c;
  private Partitioner<K> p;
  private Extractor<InType, K, V> ext;
  private Observer<Tuple<Integer, OutType>> o;
  private final Observer<InType> inputObserver; 

  private final EStage<Object> outputDriver;
  private final EventHandler<Integer> doneHandler;
  
  @Inject
  public CombiningSnowshovelGrouper(Combiner<OutType, K, V> c, Partitioner<K> p, Extractor<InType, K, V> ext,
      @Parameter(StageConfiguration.StageObserver.class) Observer<Tuple<Integer, OutType>> o, 
      @Parameter(StageConfiguration.NumberOfThreads.class) int outputThreads,
      @Parameter(StageConfiguration.StageName.class) String stageName,
      @Parameter(ContinuousStage.PeriodNS.class) long outputPeriod_ns) {
    this.c = c;
    this.p = p;
    this.ext = ext;
    this.o = o;
    // calling this.new on a @Unit's inner class without its own state is currently the same as Tang injecting it
    this.outputDriver = new ContinuousStage<Object>(this.new OutputImpl(), outputThreads, stageName, outputPeriod_ns);
    this.doneHandler = ((ContinuousStage<Object>)outputDriver).getDoneHandler();
    register = new ConcurrentSkipListMap<>();
    inputDone = false;
    this.inputObserver = this.new InputImpl();

    // there is no dependence from input finish to output start
    // The alternative placement of this event is in the first call to onNext,
    // but Output onNext already provides blocking
    //outputReady.onNext(new GrouperEvent());
  }

  @Inject
  public CombiningSnowshovelGrouper(Combiner<OutType, K, V> c, Partitioner<K> p, Extractor<InType, K, V> ext,
      @Parameter(StageConfiguration.StageObserver.class) Observer<Tuple<Integer, OutType>> o, 
      @Parameter(StageConfiguration.NumberOfThreads.class) int outputThreads,
      @Parameter(StageConfiguration.StageName.class) String stageName) {
    this(c, p, ext, o, outputThreads, stageName, 0);
  }

  @Inject
  public CombiningSnowshovelGrouper(Combiner<OutType, K, V> c, Partitioner<K> p, Extractor<InType, K, V> ext,
      @Parameter(StageConfiguration.StageObserver.class) Observer<Tuple<Integer, OutType>> o, 
      @Parameter(StageConfiguration.NumberOfThreads.class) int outputThreads) {
    this(c, p, ext, o, outputThreads, CombiningSnowshovelGrouper.class.getName()+"-Stage");
  }
 
  private interface Input<T> extends Observer<T>{}
  private class InputImpl implements Input<InType> {
    @Override
    public void onCompleted() {
      synchronized (register) {
        inputDone = true;
        register.notifyAll();
      }
    }

    @Override
    public void onError(Exception arg0) {
      // TODO
      throw new NotImplementedException(arg0);
    }

    @Override
    public void onNext(InType datum) {
      V oldVal;
      V newVal;

      final K key = ext.key(datum);
      final V val = ext.value(datum);

      // try combining atomically until succeed
      boolean succ = false;
      boolean mayHaveFilled = true; // conservative flag that says whether we might have made the map go from empty to not empty
      oldVal = register.get(key);
      do {
        if (oldVal == null) {
          succ = (null == (oldVal = register.putIfAbsent(key, val)));
          if (succ) {
            if (LOG.isLoggable(Level.FINER)) LOG.finer("input key:"+key+" val:"+val+" (new)");
            break;
          }
        } else {
          newVal = c.combine(key, oldVal, val);
          succ = register.replace(key, oldVal, newVal);
          if (!succ)
            oldVal = register.get(key);
          else {
            if (LOG.isLoggable(Level.FINER)) LOG.finer("input key:"+key+" val:"+val+" -> newVal:"+newVal);
            mayHaveFilled = false;
            break;
          }
        }
      } while (true);

      // TODO: make less conservative
      if (mayHaveFilled) {
        synchronized (register) {
          register.notify();
        }
      }
     
      /*if (val instanceof Integer) {
        LOG.info("snow shovel size "+register.size());
      }*/

      // notify at least the first time that consuming is possible
      outputDriver.onNext(new GrouperEvent());
      
    }   
  }

  
 
  private interface Output<T> extends Observer<T> {}
  private class OutputImpl implements Output<Integer> { //TODO: could change Integer to a StageContext type since no effect
    @Override
    public void onCompleted() {
      if (!register.isEmpty() && inputDone) {
        throw new IllegalStateException("Output channel cannot complete before outputting is finished");
      }

      o.onCompleted();
    }

    @Override
    public void onError(Exception ex) {
      // TODO Auto-generated method stub
      throw new UnsupportedOperationException(ex);
    }

    /**
     * Best effort flush of current storage to output. Blocks until it flushes
     * something or until eventually after {@code InObserver.onCompleted()}
     * has been called.
     */
    @Override
    public void onNext(Integer threadId) {
      boolean flushedSomething = false;
      do {
        // quick check for empty
        if (register.isEmpty()) {
          // if it may be empty now then wait until filled
          boolean cachedInputDone = false;
          synchronized (register) {
            // if observed empty and done then finished outputting
            cachedInputDone = inputDone;

            while (register.isEmpty() && !inputDone) {
              try {
                //long tag = Thread.currentThread().getId();// System.nanoTime();
                //LOG.finer("output side waits "+tag);
                register.wait();
                //LOG.finer("output side wakes "+tag);
              } catch (InterruptedException e) {
                throw new IllegalStateException(e);
              }
            }
          }
          if (cachedInputDone) {
            doneHandler.onNext(threadId);
            return;
          }
        }
        
        Map.Entry<K, V> e_cursor = register.pollFirstEntry();
        Tuple<K, V> cursor = (e_cursor == null) ? null : new Tuple<>(e_cursor.getKey(), e_cursor.getValue());
        while (cursor != null) {
          if (cursor.getValue() != null) {
            o.onNext(new Tuple<>(p.partition(cursor.getKey()), c.generate(cursor.getKey(), cursor.getValue())));
            flushedSomething = true;
          }

          K nextKey = register.higherKey(cursor.getKey());

          // remove may return null if another thread interleaved a removal
          cursor = (nextKey == null) ? null : new Tuple<>(nextKey, register.remove(nextKey));
        }
      } while (!flushedSomething);

    }
  }
  
  @Override
  public String toString() {
    return "register: "+register;
  }

  @Override
  public void close() throws Exception {
    this.outputDriver.close();
  }

  @Override
  public void onCompleted() {
    inputObserver.onCompleted();
  }
  @Override
  public void onError(Exception arg0) {
    inputObserver.onCompleted();
  }
  @Override
  public void onNext(InType arg0) {
    inputObserver.onNext(arg0);
  }
}
