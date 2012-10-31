package com.microsoft.tang;

import java.io.OutputStream;

/**
 * TANG Configuration object.
 * 
 * Tang Configuration objects are immutable and constructed via
 * ConfigurationBuilders.
 * 
 * @author sears
 */
public interface Configuration {

  /**
   * Writes this Configuration to the given OutputStream.
   * 
   * @param s
   * @throws IOException
   */
  public void writeConfigurationFile(OutputStream s);

}