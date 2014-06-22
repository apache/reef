package com.microsoft.reef.webserver;

import com.microsoft.tang.annotations.DefaultImplementation;

/**
 * Interface for DriverInfoSerializer
 */
@DefaultImplementation(AvroDriverInfoSerializer.class)
public interface DriverInfoSerializer {
  /**
   * Build AvroDriverInfo object from raw data
   *
   * @param id
   * @param startTime
   * @return AvroDriverInfo object
   */
  public AvroDriverInfo toAvro(final String id, final String startTime);

  /**
   * Convert AvroDriverInfo object to JSon string
   *
   * @param avroDriverInfo
   * @return
   */
  public String toString(final AvroDriverInfo avroDriverInfo);
}
