package com.microsoft.reef.evaluator.context;

import com.microsoft.wake.EventHandler;

public interface ContextMessageHandler extends EventHandler<byte[]> {

  /**
   * @param message sent by the driver to this context
   */
  @Override
  public void onNext(final byte[] message);
}
