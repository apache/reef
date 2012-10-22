package com.example;

import javax.inject.Inject;

import com.microsoft.tang.InjectionPlan;
import com.microsoft.tang.Tang;
import com.microsoft.tang.TypeHierarchy;
import com.microsoft.tang.annotations.Name;
import com.microsoft.tang.annotations.NamedParameter;
import com.microsoft.tang.annotations.Parameter;

public class Timer {
  @NamedParameter(default_value="10", type=int.class,
      doc="Number of seconds to sleep", short_name="sec")
  class Seconds implements Name {}
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
    typeHierarchy.writeJson(System.err);
    Tang tang = new Tang(typeHierarchy);
    InjectionPlan ip = tang.getInjectionPlan("com.example.Timer");
    System.out.println(InjectionPlan.prettyPrint(ip));
    System.out.println("Number of plans:" + ip.getNumAlternatives());
    Timer timer = tang.getInstance(Timer.class);
    System.out.println("Tick...");
    timer.sleep();
    System.out.println("Tock.");
  }
}
