package com.microsoft.wake.time.runtime;

import com.microsoft.tang.annotations.DefaultImplementation;

@DefaultImplementation(RealTimer.class)
public interface Timer {
  long getCurrent();

  long getDuration(final long time);

  boolean isReady(final long time);
}
