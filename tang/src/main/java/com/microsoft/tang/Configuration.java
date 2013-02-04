package com.microsoft.tang;

import java.io.File;
import java.io.IOException;
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
  @Deprecated
  public void writeConfigurationFile(OutputStream s);

  /**
   * Writes this Configuration to the given OutputStream.
   * 
   * @throws IOException
   * 
   */
  public void writeConfigurationFile(File f) throws IOException;

  /**
   * Return a String representation of this Configuration that is suitable for
   * parsing by ConfigurationBuilder.addConfiguration
   */
  public String toConfigurationString();
}