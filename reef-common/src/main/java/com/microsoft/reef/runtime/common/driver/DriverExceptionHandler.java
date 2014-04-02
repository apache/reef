package com.microsoft.reef.runtime.common.driver;

import com.microsoft.reef.annotations.audience.DriverSide;
import com.microsoft.reef.annotations.audience.Private;
import com.microsoft.reef.util.Optional;
import com.microsoft.wake.EventHandler;

import javax.inject.Inject;

/**
 * Exception handler for exceptions thrown by client code in the Driver.
 * It uses the ClientJobStatusHandler to send an update to the client and die.
 */
@Private
@DriverSide
public final class DriverExceptionHandler implements EventHandler<Throwable> {

  /**
   * We delegate the failures to this object.
   */
  private final ClientJobStatusHandler clientJobStatusHandler;

  @Inject
  public DriverExceptionHandler(final ClientJobStatusHandler clientJobStatusHandler) {
    this.clientJobStatusHandler = clientJobStatusHandler;
  }


  @Override
  public void onNext(final Throwable throwable) {
    this.clientJobStatusHandler.close(Optional.of(throwable));
  }
}
