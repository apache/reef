package com.microsoft.tang.formats;

import com.microsoft.tang.Configuration;
import com.microsoft.tang.test.RoundTripTest;

/**
 * A test for Configuration serialization to Strings using AvroConfigurationSerializer.
 */
public class AvroConfigurationSerializerStringRoundtripTest extends RoundTripTest {
  @Override
  public Configuration roundTrip(final Configuration configuration) throws Exception {
    final AvroConfigurationSerializer serializer = new AvroConfigurationSerializer();
    return serializer.fromString(serializer.toString(configuration));
  }
}
