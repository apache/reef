/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.reef.wake.storage;

import java.io.File;
import java.net.URI;
import java.net.URISyntaxException;

public class FileIdentifier implements StorageIdentifier {
  private final File f;

  public FileIdentifier(final String s) throws URISyntaxException {
    f = new File(new URI(s));
  }

  @Override
  public String toString() {
    return f.toString();
  }

  @Override
  public boolean equals(final Object o) {
    if (!(o instanceof FileIdentifier)) {
      return false;
    }
    return f.equals(((FileIdentifier) o).f);
  }

  @Override
  public int hashCode() {
    return f.hashCode();
  }
}
