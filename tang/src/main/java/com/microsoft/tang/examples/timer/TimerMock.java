package com.microsoft.tang.examples.timer;

import javax.inject.Inject;

import com.microsoft.tang.Configuration;
import com.microsoft.tang.Tang;
import com.microsoft.tang.annotations.Parameter;
import com.microsoft.tang.exceptions.BindException;
import com.microsoft.tang.exceptions.InjectionException;
import com.microsoft.tang.formats.ConfigurationModule;
import com.microsoft.tang.formats.ConfigurationModuleBuilder;
import com.microsoft.tang.formats.OptionalParameter;

public class TimerMock implements Timer {

  public static class TimerMockConf extends ConfigurationModuleBuilder {
    public static final OptionalParameter<Integer> MOCK_SLEEP_TIME = new OptionalParameter<>();
  }
  public static final ConfigurationModule CONF = new TimerMockConf()
    .bindImplementation(Timer.class, TimerMock.class)
    .bindNamedParameter(Timer.Seconds.class, TimerMockConf.MOCK_SLEEP_TIME)
    .build();
  
  private final int seconds;
  
  @Inject
  TimerMock(@Parameter(Timer.Seconds.class) int seconds) {
    if(seconds < 0) {
      throw new IllegalArgumentException("Cannot sleep for negative time!");
    }
    this.seconds = seconds; 
  }
  @Override
  public void sleep() {
    System.out.println("Would have slept for " + seconds + "sec.");
  }

  public static void main(String[] args) throws BindException, InjectionException, Exception {
    Configuration c = TimerMock.CONF
      .set(TimerMockConf.MOCK_SLEEP_TIME, 1)
      .build();
    Timer t = Tang.Factory.getTang().newInjector(c).getInstance(Timer.class);
    System.out.println("Tick...");
    t.sleep();
    System.out.println("...tock.");
  }
}
