package com.example;

import javax.inject.Inject;

import com.microsoft.tang.annotations.Name;
import com.microsoft.tang.annotations.NamedParameter;
import com.microsoft.tang.annotations.Parameter;
import com.microsoft.tang.implementation.ConfigurationBuilderImpl;
import com.microsoft.tang.implementation.InjectionPlan;
import com.microsoft.tang.implementation.ConfigurationImpl;
import com.microsoft.tang.implementation.InjectorImpl;
import com.microsoft.tang.implementation.TypeHierarchy;

public class Timer {
  @NamedParameter(default_value="10",
      doc="Number of seconds to sleep", short_name="sec")
  class Seconds implements Name<Integer> {}
  private final int seconds;

  @Inject
  public Timer(@Parameter(Seconds.class) int seconds) {
    this.seconds = seconds;
  }

  public void sleep() throws Exception {
    java.lang.Thread.sleep(seconds * 1000);
  }
  
  public static void main(String[] args) throws Exception {
    TypeHierarchy typeHierarchy = new TypeHierarchy();
    typeHierarchy.register(Timer.class);
    ConfigurationBuilderImpl tang = new ConfigurationBuilderImpl();
    tang.register(Timer.class);
    ConfigurationImpl conf = tang.build();
    InjectorImpl injector = conf.injector();
    InjectionPlan<Timer> ip = injector.getInjectionPlan(Timer.class);
    System.out.println(ip.toPrettyString());
    System.out.println("Number of plans:" + ip.getNumAlternatives());
    Timer timer = injector.getInstance(Timer.class);
    System.out.println("Tick...");
    timer.sleep();
    System.out.println("Tock.");
  }
}
