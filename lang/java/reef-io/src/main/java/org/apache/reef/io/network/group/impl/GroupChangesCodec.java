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
package org.apache.reef.io.network.group.impl;

import org.apache.reef.io.network.group.api.GroupChanges;
import org.apache.reef.io.serialization.Codec;

import javax.inject.Inject;
import java.util.logging.Level;
import java.util.logging.Logger;

public class GroupChangesCodec implements Codec<GroupChanges> {
  private static final Logger LOG = Logger.getLogger(GroupChangesCodec.class.getName());

  @Inject
  public GroupChangesCodec() {
  }

  @Override
  public GroupChanges decode(final byte[] changeBytes) {
    return new GroupChangesImpl(changeBytes[0] == 1);
  }

  @Override
  public byte[] encode(final GroupChanges changes) {
    final byte[] retVal = new byte[1];
    if (changes.exist()) {
      retVal[0] = 1;
    }
    return retVal;
  }

  public static void main(final String[] args) {
    GroupChanges changes = new GroupChangesImpl(false);
    final GroupChangesCodec changesCodec = new GroupChangesCodec();
    GroupChanges changes1 = changesCodec.decode(changesCodec.encode(changes));
    test(changes, changes1);
    changes = new GroupChangesImpl(true);
    changes1 = changesCodec.decode(changesCodec.encode(changes));
    test(changes, changes1);
  }

  private static void test(final GroupChanges changes, final GroupChanges changes1) {
    final boolean c1 = changes.exist();
    final boolean c2 = changes1.exist();

    if (c1 != c2) {
      LOG.log(Level.SEVERE, "Something is wrong: {0} != {1}", new Object[]{c1, c2});
    } else {
      LOG.log(Level.INFO, "Codec is fine");
    }
  }
}
