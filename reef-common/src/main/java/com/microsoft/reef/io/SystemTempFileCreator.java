package com.microsoft.reef.io;

import javax.inject.Inject;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.attribute.FileAttribute;

/**
 * A TempFileCreator that uses the system's temp directory.
 */
public final class SystemTempFileCreator implements TempFileCreator {

  @Inject
  public SystemTempFileCreator() {
  }

  @Override
  public File createTempFile(final String prefix, final String suffix) throws IOException {
    return File.createTempFile(prefix, suffix);
  }

  @Override
  public File createTempDirectory(final String prefix, final FileAttribute<?> attrs) throws IOException {
    return Files.createTempDirectory(prefix, attrs).toFile();
  }
}
