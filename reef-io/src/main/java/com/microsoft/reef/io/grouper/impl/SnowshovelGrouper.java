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

import java.util.logging.Logger;

import javax.inject.Inject;

import org.apache.commons.lang.NotImplementedException;

import com.microsoft.reef.io.Tuple;
import com.microsoft.reef.io.grouper.Grouper;
import com.microsoft.tang.annotations.Parameter;
import com.microsoft.wake.StageConfiguration;
import com.microsoft.wake.rx.Observer;
import com.microsoft.wake.rx.RxStage;
import com.microsoft.wake.rx.impl.RxThreadPoolStage;

public class SnowshovelGrouper<InType, OutType, K extends Comparable<K>, V> implements Grouper<InType> {
  private Logger LOG = Logger.getLogger(SnowshovelGrouper.class.getName());
  
  private final Combiner<OutType, K, V> c;
  private Partitioner<K> p;
  private Extractor<InType, K, V> ext;
  private final Observer<InType> inputObserver; 

  private final RxStage<Tuple<Integer,OutType>> outputStage;
  
  @Inject
  public SnowshovelGrouper(Combiner<OutType, K, V> c, Partitioner<K> p, Extractor<InType, K, V> ext,
      @Parameter(StageConfiguration.StageObserver.class) Observer<Tuple<Integer, OutType>> o, 
      @Parameter(StageConfiguration.NumberOfThreads.class) int outputThreads,
      @Parameter(StageConfiguration.StageName.class) String stageName) {
    this.p = p;
    this.ext = ext;
    this.c = c;
    this.inputObserver = this.new InputImpl();
    
    this.outputStage = new RxThreadPoolStage<Tuple<Integer, OutType>>(stageName, o, outputThreads);

    // there is no dependence from input finish to output start
    // The alternative placement of this event is in the first call to onNext,
    // but Output onNext already provides blocking
    //outputReady.onNext(new GrouperEvent());
  }

  @Inject
  public SnowshovelGrouper(Combiner<OutType, K, V> c, Partitioner<K> p, Extractor<InType, K, V> ext,
      @Parameter(StageConfiguration.StageObserver.class) Observer<Tuple<Integer, OutType>> o, 
      @Parameter(StageConfiguration.NumberOfThreads.class) int outputThreads) {
    this(c, p, ext, o, outputThreads, SnowshovelGrouper.class.getName()+"-Stage");
  }
 
  private interface Input<T> extends Observer<T>{}
  private class InputImpl implements Input<InType> {
    @Override
    public void onCompleted() {
      outputStage.onCompleted();
    }

    @Override
    public void onError(Exception arg0) {
      // TODO
      throw new NotImplementedException(arg0);
    }

    @Override
    public void onNext(InType datum) {
      final K key = ext.key(datum);
      final V val = ext.value(datum);
      
      outputStage.onNext(new Tuple<>(p.partition(key), c.generate(key, val)));
    }   
  }

  @Override
  public String toString() {
    return "stored:" + outputStage;
  }

  @Override
  public void close() throws Exception {
    this.outputStage.close();
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
