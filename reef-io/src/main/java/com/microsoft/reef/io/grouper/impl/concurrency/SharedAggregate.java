package com.microsoft.reef.io.grouper.impl.concurrency;

import com.google.common.util.concurrent.AtomicDouble;


public class SharedAggregate {

  private final int localUpdatesThreshold;
  private final ThreadLocal<LocalEntry> localData;
  private final AtomicDouble sharedDelta;
  
  public SharedAggregate(int localUpdatesThreshold) {
    this.localUpdatesThreshold = localUpdatesThreshold;
    this.localData = new ThreadLocal<LocalEntry>() {
      @Override
      public LocalEntry initialValue() {
        return new LocalEntry();
      }
    };
    this.sharedDelta = new AtomicDouble(0);
  }
  
  public void updateLocal(double delta) {
    LocalEntry mine = localData.get();
    double newVal = mine.value+delta;
    int n = mine.numUpdates+1;
    
    if (n >= localUpdatesThreshold) {
      sharedDelta.addAndGet(newVal);
      mine.value = 0.0;
      mine.numUpdates = 0;
    } else {
      mine.value = newVal;
      mine.numUpdates = n;
    }
  }
  
  public double getSharedValue() {
    return sharedDelta.get();
  }
  
  private class LocalEntry {
    public double value;
    public int numUpdates;
    
    public LocalEntry() {
      value = 0.0;
      numUpdates = 0;
    }
  }
  
  //TODO: what would give pull flush ability is having the thread local thing
  // only be the index into a list. Of course, need to construct the list to
  // make false sharing unlikely
}
