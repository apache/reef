package com.microsoft.tang.examples.timer;

import com.microsoft.tang.annotations.DefaultImplementation;
import com.microsoft.tang.annotations.Name;
import com.microsoft.tang.annotations.NamedParameter;

@DefaultImplementation(TimerImpl.class)
public interface Timer {
  @NamedParameter(default_value="10",
      doc="Number of seconds to sleep", short_name="sec")
  public static class Seconds implements Name<Integer> { }
  public void sleep() throws Exception;
}
