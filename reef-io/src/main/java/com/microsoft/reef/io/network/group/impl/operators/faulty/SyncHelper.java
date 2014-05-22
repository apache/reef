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
package com.microsoft.reef.io.network.group.impl.operators.faulty;

import com.microsoft.wake.Identifier;

import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

public final class SyncHelper {

  private static final Logger LOG = Logger.getLogger(SyncHelper.class.getName());

  static void update(final Set<Identifier> childIdentifiers,
                     final Map<Identifier, Integer> childStatus,
                     final Identifier self) {

    // TODO: Currently does not care about parents
    // Assumes that all ctr msgs are about children
    // There is currently only one case where ctrl
    // msgs are sent about parents thats when the
    // task is complete or control task finishes
    // It works now because we do not depend on sync
    // functionality for that

    for (final Identifier childIdentifier : childStatus.keySet()) {
      final int status = childStatus.get(childIdentifier);
      if (status < 0) {
        LOG.log(Level.FINEST, "RedReceiver: Removing {0} from children of {1}",
            new Object[] { childIdentifier, self });
        childIdentifiers.remove(childIdentifier);
      } else if (status > 0) {
        LOG.log(Level.FINEST, "RedReceiver: Adding {0} from children of {1}",
            new Object[] { childIdentifier, self });
        LOG.log(Level.FINEST, "RedReceiver: Adding " + childIdentifier + " to children of " + self);
        childIdentifiers.add(childIdentifier);
      } else {
        //No change. Need not worry
      }
    }
  }
}
