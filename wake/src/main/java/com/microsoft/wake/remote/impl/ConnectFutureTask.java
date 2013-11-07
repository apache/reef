package com.microsoft.wake.remote.impl;

import java.util.concurrent.Callable;
import java.util.concurrent.FutureTask;

import com.microsoft.wake.EventHandler;

public class ConnectFutureTask<T> extends FutureTask<T> {

  private final EventHandler<ConnectFutureTask<T>> handler;
  
  public ConnectFutureTask(Callable<T> callable, EventHandler<ConnectFutureTask<T>> handler) {
    super(callable);
    this.handler = handler;
  }
  
  @Override
  protected void done() {
    handler.onNext(this);
  }

}
