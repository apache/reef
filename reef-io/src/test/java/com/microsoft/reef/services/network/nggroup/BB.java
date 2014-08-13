/**
 * Copyright (C) 2014 Microsoft Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.microsoft.reef.services.network.nggroup;

import java.nio.ByteBuffer;

public class BB {
  public static void main(final String[] args) {
    // May make these configurable using -Dxxx
    final int MAX_BUFFS = 1000;
    final int BUFSIZE = 300 * 1024 * 1024;
    final int PUT = 1000 * 1000;
    final int REPORT = 1;
    final boolean store = false;
    final byte[] src = new byte[BUFSIZE];
    for (int i = 0; i < MAX_BUFFS; i++) {
      final ByteBuffer bb = ByteBuffer.allocateDirect(BUFSIZE);
      bb.put(src);
      if (i % REPORT == 0) {
        System.err.println("Done " + (i + 1));
      }
    }
  }
}
