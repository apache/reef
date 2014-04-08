package com.microsoft.reef.runtime.common.driver;

/**
 * The status of the Driver.
 */
public enum DriverStatus {
  PRE_INIT,
  INIT,
  RUNNING,
  SHUTTING_DOWN,
  FAILING
}
