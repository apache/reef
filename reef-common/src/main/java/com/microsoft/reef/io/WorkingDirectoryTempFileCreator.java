package com.microsoft.reef.io;

import com.microsoft.reef.annotations.Provided;

import javax.inject.Inject;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.attribute.FileAttribute;

/**
 * Creates temp files in a directory named "temp" within  the current working directory.
 */
@Provided
public final class WorkingDirectoryTempFileCreator implements TempFileCreator {

  private final File tempFolder;

  @Inject
  public WorkingDirectoryTempFileCreator() throws IOException {
    final File workingDirectory = new File(".");
    this.tempFolder = Files.createTempDirectory(workingDirectory.toPath(), "temp").toFile();
  }


  @Override
  public File createTempFile(final String prefix, final String suffix) throws IOException {
    return File.createTempFile(prefix, suffix, this.tempFolder);
  }

  @Override
  public File createTempDirectory(final String prefix, final FileAttribute<?> attrs) throws IOException {
    return Files.createTempDirectory(this.tempFolder.toPath(), prefix, attrs).toFile();
  }
}
