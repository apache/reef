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

import javax.inject.Inject;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

public class CombiningBlockingGrouper<InType, OutType, K extends Comparable<K>, V> implements Grouper<InType> {
  private static final Logger LOG = Logger.getLogger(CombiningBlockingGrouper.class.getName());
  
  private final ConcurrentSkipListMap<K, V> combined;
  private final Combiner<OutType, K, V> c;
  private final Partitioner<K> p;
  private final Extractor<InType, K, V> ext;
  private final Observer<Tuple<Integer, OutType>> o;
  private final Observer<InType> inputObserver; 
  
  private final EStage<Object> outputDriver;
  private final EventHandler<Integer> doneHandler;
  
  ThreadLocal<Boolean> cachedInputDone = new ThreadLocal<Boolean>() {
    @Override
    protected Boolean initialValue() {
      return false;
    }
  };

  @Inject
  public CombiningBlockingGrouper(Combiner<OutType, K, V> c, Partitioner<K> p, Extractor<InType, K, V> ext,
      @Parameter(StageConfiguration.StageObserver.class) Observer<Tuple<Integer, OutType>> o, 
      @Parameter(StageConfiguration.NumberOfThreads.class) int outputThreads) {
    this.c = c;
    this.p = p;
    this.ext = ext;
    this.o = o;
    this.inputObserver = this.new InputImpl();
    this.outputDriver = new ContinuousStage<Object>(this.new OutputImpl(), outputThreads);
    this.doneHandler = ((ContinuousStage<Object>)outputDriver).getDoneHandler();
    combined = new ConcurrentSkipListMap<>();
  }

  private interface Input<T> extends Observer<T>{}
  private class InputImpl implements Input<InType> {
    @Override
    public void onCompleted() {
      outputDriver.onNext(new GrouperEvent());
    }

    @Override
    public void onError(Exception ex) {
      // TODO: implement
      throw new UnsupportedOperationException(ex);
    }


      /**
       * Insert an element into the Grouper.
       * Note that if {@code Combiner.keyFinished} can return true
       * then the caller must ensure for a given key k:
       * <br>1. ordering from the first {@code onNext(k)} to all following {@code onNext(k)}
       * <br>2. ordering from the last {@code onNext(k)} (the out put triggering one) to the following {@code onNext(k)}
       * 
       * <br>These requirements are meant to support usages where the
       * first onNext(k) initializes some state, e.g. a countdown.
       */
      @Override
      public void onNext(InType datum) {
        // More efficient implementations are possible when
        // this method knows the number of inputting threads
        // or by having a greater number of concurrent maps picked
        // by threadid%num.
        // A separate map can be used for each and combined
        // when transitioning to read mode.

        // Logger.getLogger(this.getClass().getName()).log(Level.FINEST,
        // "Grouper receives input " + datum);
        V oldVal;
        V newVal;

        final K key = ext.key(datum);
        final V val = ext.value(datum);

        // try combining atomically until succeed
        newVal = val;
        oldVal = combined.get(key);
        boolean succ = oldVal == null && (null == (oldVal = combined.putIfAbsent(key, newVal)));

        if (!succ) {
          // The presence of a key is monotonic for blocking Groupers,
          // so we now only try replacing the value
          newVal = c.combine(key, oldVal, val);
          while (!combined.replace(key, oldVal, newVal)) {
            oldVal = combined.get(key);
            newVal = c.combine(key, oldVal, val);
          }
        }

        // This is not a time of check to time of use bug because
        // the interface requires caller synchronization between first onNext(k') and filling onNext(k')
        if (c.keyFinished(key, newVal)) {
          output(key, combined.remove(key));
        }
      }
    }
  
  private void output(K key, V val) {
    o.onNext(new Tuple<>(p.partition(key), c.generate(key, val)));
  }
  
  

  private interface Output<T> extends Observer<T> {}
  private class OutputImpl implements Output<Integer> {
    private AtomicInteger i = new AtomicInteger(0);
    @Override
    public void onCompleted() {
      o.onCompleted();
    }

    @Override
    public void onError(Exception ex) {
      // TODO: implement
      throw new UnsupportedOperationException(ex);
    }

    @Override
    public void onNext(Integer threadId) {
      i.incrementAndGet();

      // TODO: more efficient versions are possible,
      // such as making a total key count available and allowing the driver
      // to do tapered load balancing, etc. Amortize the cost of the atomic
      // pollFirstEntry
      Entry<K, V> element = combined.pollFirstEntry();
      // Logger.getLogger(this.getClass().getName()).log(Level.FINEST,
      // "combined left " + combined.size());
      if (element != null) {
        output(element.getKey(), element.getValue());
      } else {
        LOG.fine("thread " + threadId + " calls doneHandler when "+combined.size());
        doneHandler.onNext(threadId);
      }
    }
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
  
  
  @Override
  public void close() throws Exception {
    this.outputDriver.close();
  }

  @Override
  public String toString() {
    return "register: "+combined;
  }

}
