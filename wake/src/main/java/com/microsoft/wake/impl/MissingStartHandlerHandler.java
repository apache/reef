package com.microsoft.wake.impl;

import com.microsoft.wake.EventHandler;
import com.microsoft.wake.time.event.StartTime;

import javax.inject.Inject;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * The EventHandler used as the default for the Clock.StartHandler event.
 */
public class MissingStartHandlerHandler implements EventHandler<StartTime> {

  @Inject
  private MissingStartHandlerHandler() {
  }

  @Override
  public void onNext(final StartTime value) {
    Logger.getLogger(MissingStartHandlerHandler.class.toString())
        .log(Level.WARNING,
            "No binding to Clock.StartHandler. It is likely that the clock will immediately go idle and close.");
  }
}
