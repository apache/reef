package org.apache.reef.exception;

import org.apache.reef.annotations.audience.DriverSide;

/**
 * Created by anchung on 5/4/2016.
 */
@DriverSide
public final class NonSerializableException extends Exception {
  private final byte[] error;

  public NonSerializableException(final String message, final byte[] error) {
    super(message);
    this.error = error;
  }

  public byte[] getError() {
    return error;
  }
}
