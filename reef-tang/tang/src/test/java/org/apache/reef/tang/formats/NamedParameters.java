package org.apache.reef.tang.formats;

import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;

/**
 * Named parameters to be used in tests.
 */
public class NamedParameters {

  @NamedParameter(short_name = AString.SHORT_NAME, default_value = AString.DEFAULT_VALUE)
  public static class AString implements Name<String> {
    public static final String SHORT_NAME = "string";
    public static final String DEFAULT_VALUE = "default";
  }
}
