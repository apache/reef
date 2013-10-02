package com.microsoft.tang.examples.timer;

import javax.inject.Inject;

import com.microsoft.tang.annotations.Parameter;

public class TimerImpl implements Timer {

  private final int seconds;
  @Inject
  public TimerImpl(@Parameter(Timer.Seconds.class) int seconds) {
    if(seconds < 0) {
      throw new IllegalArgumentException("Cannot sleep for negative time!");
    }
    this.seconds = seconds;
  }
  @Override
  public void sleep() throws Exception {
    java.lang.Thread.sleep(seconds);
  }

}
