package org.apache.reef.util;

import javax.inject.Inject;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Version information, retrieved from the pom (via a properties file reference)
 */
public final class Version {

  private final static Logger LOG = Logger.getLogger(Version.class.getName());

  private final static String FILENAME = "version.properties";
  private final static String VERSION_KEY = "version";
  private final static String VERSION_DEFAULT = "unknown";

  private final String version;

  @Inject
  public Version() {
    this.version = initVersion();
  }

  private static String initVersion() {
    String version;
    try (final InputStream is = Thread.currentThread().getContextClassLoader().getResourceAsStream(FILENAME)) {
      if (is == null) {
        throw new IOException(FILENAME+" not found");
      }
      final Properties properties = new Properties();
      properties.load(is);
      version = properties.getProperty(VERSION_KEY, VERSION_DEFAULT);
    } catch (IOException e) {
      LOG.log(Level.WARNING, "Could not find REEF version");
      version = VERSION_DEFAULT;
    }
    return version;
  }

  /**
   * @return the version string for REEF.
   */
  public String getVersion() {
    return version;
  }
}
