package com.microsoft.tang;

import javax.inject.Inject;

import org.junit.Test;

import com.microsoft.tang.annotations.Name;
import com.microsoft.tang.annotations.NamedParameter;
import com.microsoft.tang.annotations.Parameter;
import com.microsoft.tang.exceptions.BindException;
import com.microsoft.tang.exceptions.InjectionException;

import junit.framework.Assert;

public class TestInjectionFuture {
  @Test
  public void testFutures() throws InjectionException, BindException {
    JavaConfigurationBuilder cb = Tang.Factory.getTang().newConfigurationBuilder();
    cb.bindSingleton(FlyingCar.class);
    cb.bindSingleton(Futurist.class);
    Injector i = Tang.Factory.getTang().newInjector(cb.build());
    
    Futurist f = i.getInstance(Futurist.class);
    Assert.assertTrue(f == f.getMyCar().getDriver());
    Assert.assertTrue(f.getMyCar() == f.getMyCar().getDriver().getMyCar());
  }
  @Test
  public void testFutures2() throws InjectionException, BindException {
    JavaConfigurationBuilder cb = Tang.Factory.getTang().newConfigurationBuilder();
    cb.bindSingleton(FlyingCar.class);
    cb.bindSingleton(Futurist.class);
    Injector i = Tang.Factory.getTang().newInjector(cb.build());
    
    FlyingCar c = i.getInstance(FlyingCar.class);
    Assert.assertTrue(c == c.getDriver().getMyCar());
    Assert.assertTrue(c.getDriver() == c.getDriver().getMyCar().getDriver());
  }
  
  public static class Futurist {
    private final InjectionFuture<FlyingCar> f_car;
    @Inject
    public Futurist(InjectionFuture<FlyingCar> car) {
      this.f_car = car;
    }
    public FlyingCar getMyCar() {
      FlyingCar c = f_car.get();
      return c;
    }
    
  }
  public static class FlyingCar {
    private final String color;
    private final Futurist driver;
    @NamedParameter(default_value="blue")
    class Color implements Name<String> { }
    @Inject
    FlyingCar(@Parameter(Color.class) String color, Futurist driver) {
      this.color = color;
      this.driver = driver;
    }
    public String getColor() {
      return color;
    }
    public Futurist getDriver() {
      return driver;
    }
  }
}
