package com.microsoft.reef.io.grouper.impl.concurrency;

import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;

public class SharedPullCounter {
 
  private final AtomicLongArray counters; //TODO: check implementation for cache line padding
  private final long mask;
  
  private boolean powerOf2(int x) {
    return (x & (x - 1)) == 0;
  }

  public SharedPullCounter(int concurrency) {
    if (concurrency < 1 || !powerOf2(concurrency)) throw new IllegalArgumentException("Only powers of two allowed");
    
    mask = concurrency-1;
    this.counters = new AtomicLongArray(concurrency);
  }

  public void add(long x) {
    this.counters.getAndAdd(myId(), x);
  }
  
  public void addSpecific(int i, long x) {
    this.counters.getAndAdd(i, x);
  }
  
  public int myId() {
    return (int)(Thread.currentThread().getId() & mask);
  }
  
  public long getTotal() {
    long sum = 0;
    for (int i=0; i<counters.length(); i++) {
      sum+=counters.get(i);
    }
    return sum;
  }
}

class SharedPullCounterHash {
  
  private final ConcurrentHashMap<Long,AtomicLong> counters; //TODO: check implementation for cache line padding
 

  public SharedPullCounterHash(int concurrency) {
    this.counters = new ConcurrentHashMap<>(concurrency);
  }

  public void add(long x) {
    long id = Thread.currentThread().getId();
    AtomicLong maybe = new AtomicLong(1);
    AtomicLong old = counters.putIfAbsent(id, maybe);
    if (old!=null) old.incrementAndGet();
  }

  public long getTotal() {
    long sum = 0;
    for (AtomicLong v : counters.values()) {
      sum+=v.get();
    }
    return sum;
  }
}
