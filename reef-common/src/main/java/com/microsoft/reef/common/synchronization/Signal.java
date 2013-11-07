package com.microsoft.reef.common.synchronization;

import com.microsoft.reef.annotations.audience.Private;

@Private
class Signal { 
  private boolean done;

  public synchronized void signal() {
    done = true;
    this.notify();
  }

  public synchronized void waitFor() throws InterruptedException {
    while (!done) {
      this.wait();
    }
  }
}