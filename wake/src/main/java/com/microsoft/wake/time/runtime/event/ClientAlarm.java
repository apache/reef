package com.microsoft.wake.time.runtime.event;

import com.microsoft.wake.EventHandler;
import com.microsoft.wake.time.Time;
import com.microsoft.wake.time.event.Alarm;

public final class ClientAlarm extends Alarm {

  public ClientAlarm(final long timestamp, final EventHandler<Alarm> handler) {
    super(timestamp, handler);
  }
}
