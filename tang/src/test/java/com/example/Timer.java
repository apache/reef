package com.example;

import javax.inject.Inject;

import com.microsoft.tang.impl.InjectionPlan;
import com.microsoft.tang.impl.Tang;
import com.microsoft.tang.impl.TangConf;
import com.microsoft.tang.impl.TangInjector;
import com.microsoft.tang.impl.TypeHierarchy;
import com.microsoft.tang.annotations.Name;
import com.microsoft.tang.annotations.NamedParameter;
import com.microsoft.tang.annotations.Parameter;

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
    Tang tang = new Tang();
    tang.register(Timer.class);
    TangConf conf = tang.forkConf();
    TangInjector injector = conf.injector();
    InjectionPlan<Timer> ip = injector.getInjectionPlan(Timer.class);
    System.out.println(ip.toPrettyString());
    System.out.println("Number of plans:" + ip.getNumAlternatives());
    Timer timer = injector.getInstance(Timer.class);
    System.out.println("Tick...");
    timer.sleep();
    System.out.println("Tock.");
  }
}
