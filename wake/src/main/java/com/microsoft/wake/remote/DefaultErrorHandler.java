package com.microsoft.wake.remote;

import com.microsoft.wake.EventHandler;

import javax.inject.Inject;

/**
 * The default RemoteConfiguration.ErrorHandler
 */
final class DefaultErrorHandler implements EventHandler<Throwable> {

  @Inject
  DefaultErrorHandler() {
  }

  @Override
  public void onNext(Throwable value) {
    throw new RuntimeException("No error handler bound for RemoteManager.", value);
  }
}
