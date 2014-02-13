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

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.inject.Inject;

import org.apache.commons.lang.NotImplementedException;

import com.microsoft.reef.io.Tuple;
import com.microsoft.reef.io.grouper.Grouper;
import com.microsoft.reef.io.grouper.GrouperEvent;
import com.microsoft.tang.annotations.Parameter;
import com.microsoft.wake.EStage;
import com.microsoft.wake.EventHandler;
import com.microsoft.wake.StageConfiguration;
import com.microsoft.wake.rx.AbstractRxStage;
import com.microsoft.wake.rx.Observer;

public class AppendingSnowshovelGrouper<InType, OutType, K extends Comparable<K>, V> extends AbstractRxStage<InType> implements Grouper<InType> {
  
  private Logger LOG = Logger.getLogger(AppendingSnowshovelGrouper.class.getName());
  
  private ConcurrentSkipListMap<K, AppendingEntry<V>> register;
  private volatile boolean inputDone;
  private Combiner<OutType,K, List<V>> c;
  private Partitioner<K> p;
  private Extractor<InType, K, V> ext;
  private Observer<Tuple<Integer, OutType>> o;
  private final Observer<InType> inputObserver; 

  private final EStage<Object> outputDriver;
  private final EventHandler<Integer> doneHandler;
  private final AtomicInteger sleeping;
  
  @Inject
  public AppendingSnowshovelGrouper(Combiner<OutType, K, List<V>> c, Partitioner<K> p, Extractor<InType, K, V> ext,
      @Parameter(StageConfiguration.StageObserver.class) Observer<Tuple<Integer, OutType>> o, 
      @Parameter(StageConfiguration.NumberOfThreads.class) int outputThreads,
      @Parameter(StageConfiguration.StageName.class) String stageName,
      @Parameter(ContinuousStage.PeriodNS.class) long outputPeriod_ns) {
    super(stageName);
    this.c = c;
    this.p = p;
    this.ext = ext;
    this.o = o;
    // calling this.new on a @Unit's inner class without its own state is currently the same as Tang injecting it
    this.outputDriver = new ContinuousStage<Object>(this.new OutputImpl(), outputThreads, stageName+"-out", outputPeriod_ns);
    this.doneHandler = ((ContinuousStage<Object>)outputDriver).getDoneHandler();
    register = new ConcurrentSkipListMap<>();
    inputDone = false;
    this.inputObserver = this.new InputImpl();

    this.sleeping = new AtomicInteger();

    // there is no dependence from input finish to output start
    // The alternative placement of this event is in the first call to onNext,
    // but Output onNext already provides blocking
    outputDriver.onNext(new GrouperEvent());
  }

  @Inject
  public AppendingSnowshovelGrouper(Combiner<OutType, K, List<V>> c, Partitioner<K> p, Extractor<InType, K, V> ext,
      @Parameter(StageConfiguration.StageObserver.class) Observer<Tuple<Integer, OutType>> o, 
      @Parameter(StageConfiguration.NumberOfThreads.class) int outputThreads) {
    this(c, p, ext, o, outputThreads, AppendingSnowshovelGrouper.class.getName()+"-Stage", 0);
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
      AppendingEntry<V> oldVal;

      final K key = ext.key(datum);
      final V val = ext.value(datum);

      // try combining atomically until succeed
      boolean succ = false;
      // conservative flag that says whether we might have made the map go from empty to not empty,
      // meaning that some output threads may be waiting
      oldVal = register.get(key);
      do {
        if (oldVal == null) {
          // try to atomically put a new queue with the element already inserted
          AppendingEntry<V> newVal = new AppendingEntry<>(val);
          succ = (null == (oldVal = register.putIfAbsent(key, newVal)));
          if (succ) {
            if (LOG.isLoggable(Level.FINER)) LOG.finer("input key:"+key+" val:"+val+" (new)");
            break;
          }
        } else {
          succ = oldVal.appendIfOpen(val);
          if (!succ)
            oldVal = register.get(key);
          else {
            if (LOG.isLoggable(Level.FINER)) LOG.finer("input key:"+key+" val:"+val);
            break;
          }
        }
      } while (true);

      // TODO: make less conservative
      if (sleeping.get() > 0) {
        synchronized (register) {
          register.notify();
        }
      }
     
      // notify at least the first time that consuming is possible
      //outputDriver.onNext(new GrouperEvent());
      
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
          sleeping.incrementAndGet();
          synchronized (register) {
            // if observed empty and done then finished outputting

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
          sleeping.decrementAndGet();
          if (inputDone) {
            doneHandler.onNext(threadId);
            return;
          }
        }
        
        Map.Entry<K, AppendingEntry<V>> e_cursor = register.pollFirstEntry();
        Tuple<K, AppendingEntry<V>> cursor = (e_cursor == null) ? null : new Tuple<>(e_cursor.getKey(), e_cursor.getValue());
        while (cursor != null) {
          AppendingEntry<V> outEntry = cursor.getValue();
          if (outEntry != null) {
            // close the queue, claiming the elements
            List<V> outList = outEntry.closeAndRead();
            afterOnNext();
            o.onNext(new Tuple<>(p.partition(cursor.getKey()), c.generate(cursor.getKey(), outList)));
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
    beforeOnNext();
    inputObserver.onNext(arg0);
  }

  /**
    * Each map entry is a queue. A consumer closes the queue before reading
    * so that it is guarenteed not to miss any elements. Since closing is
    * monotonic, this entry cannot be reused and must be discarded.
    */
  private class AppendingEntry<VV> {
    private boolean closed;
    private final List<VV> elements;
    
    public AppendingEntry(VV firstElement) {
      closed = false;
      elements = new LinkedList<>();
      elements.add(firstElement);
    }
    
    public boolean appendIfOpen(VV val) {
      synchronized (this) {
        if (!closed) {
          elements.add(val);
          return true;
        } else {
          return false;
        }
      }
    }
    
    public List<VV> closeAndRead() {
      synchronized (this) {
        closed = true;
        return elements;
      }
    }
    
    public String toString() {
      return "E("+closed+", "+elements+")";
    }
  }
}
