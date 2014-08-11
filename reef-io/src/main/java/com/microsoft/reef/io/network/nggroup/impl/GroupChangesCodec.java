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
package com.microsoft.reef.io.network.nggroup.impl;

import com.microsoft.reef.io.network.nggroup.api.GroupChanges;
import com.microsoft.reef.io.serialization.Codec;

/**
 *
 */
public class GroupChangesCodec implements Codec<GroupChanges> {

  @Override
  public GroupChanges decode(final byte[] changeBytes) {
    return new GroupChangesImpl((changeBytes[0] == 1) ? true : false);
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
      System.out.println("Something is wrong");
    } else {
      System.out.println("Codec is fine");
    }
  }
}
