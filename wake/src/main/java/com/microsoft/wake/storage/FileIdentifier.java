package com.microsoft.wake.storage;

import java.io.File;
import java.net.URI;
import java.net.URISyntaxException;

public class FileIdentifier implements StorageIdentifier {
  private final File f;
  public FileIdentifier(String s) throws URISyntaxException {
    f = new File(new URI(s));
  }
  @Override
  public String toString() {
    return f.toString();
  }
  @Override
  public boolean equals(Object o) {
    if(!(o instanceof FileIdentifier)) {
      return false;
    }
    return f.equals(((FileIdentifier)o).f);
  }
  @Override
  public int hashCode() {
    return f.hashCode();
  }
}
