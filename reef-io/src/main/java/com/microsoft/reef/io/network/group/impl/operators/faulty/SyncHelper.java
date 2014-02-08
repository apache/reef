package com.microsoft.reef.io.network.group.impl.operators.faulty;

import com.microsoft.wake.Identifier;

import java.util.Map;
import java.util.Set;

/**
 *
 */
public class SyncHelper {

  static void update(final Set<Identifier> childIdentifiers, final Map<Identifier, Integer> childStatus, final Identifier self) {
    //TODO: Currently does not care about parents
    //Assumes that all ctr msgs are about children
    //There is currently only one case where ctrl
    //msgs are sent about parents thats when the
    //task is complete or control task finishes
    //It works now because we do not depend on sync
    //functionality for that
    for (final Identifier childIdentifier : childStatus.keySet()) {
      final int status = childStatus.get(childIdentifier);
      if (status < 0) {
        System.out.println("RedReceiver: Removing " + childIdentifier + " from children of " + self);
        childIdentifiers.remove(childIdentifier);
      } else if (status > 0) {
        System.out.println("RedReceiver: Adding " + childIdentifier + " to children of " + self);
        childIdentifiers.add(childIdentifier);
      } else {
        //No change. Need not worry
      }
    }
  }
}
