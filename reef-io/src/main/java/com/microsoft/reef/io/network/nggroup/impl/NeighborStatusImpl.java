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

import com.microsoft.reef.io.network.nggroup.api.NeighborStatus;
import com.microsoft.reef.io.network.proto.ReefNetworkGroupCommProtos.GroupCommMessage.Type;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Logger;

/**
 *
 */
public class NeighborStatusImpl implements NeighborStatus {

  private static final Logger LOG = Logger.getLogger(NeighborStatusImpl.class.getName());


  private final ConcurrentMap<String, StatusProcessor> neighborStatus = new ConcurrentHashMap<>();


  private final AtomicBoolean update = new AtomicBoolean(false);


  private final String name;

  public NeighborStatusImpl(final String name) {
    this.name = name;
  }

  @Override
  public void clear() {
    neighborStatus.clear();
  }

  @Override
  public void remove(final String taskId) {
    neighborStatus.remove(taskId);
  }

  @Override
  public void add(final String from, final Type type) {
    switch (type) {
      case ParentAdd:
      case ParentDead:
      case ChildAdd:
      case ChildDead:
        final StatusProcessor statusProcessor = neighborStatus.containsKey(from) ? neighborStatus.get(from) : new StatusProcessor(name);
        statusProcessor.addStatus(type);
        neighborStatus.put(from, statusProcessor);
        break;

      default:
        LOG.warning(name + type.toString() + " is not a valid type for neighborstatus");
        break;
    }
    LOG.info(name + "Handled " + type.toString() + " msg from " + from);
  }

  @Override
  public void updateDone() {
    if (this.update.compareAndSet(false, true)) {
      LOG.info(name + "Update done");
    } else {
      LOG.warning(name + "UpdateDone called when it was already marked done");
    }
  }

  @Override
  public void updateProcessed() {
    if (this.update.compareAndSet(true, false)) {
      LOG.info(name + "All updates have been processed. Resetting update to false");
    } else {
      LOG.warning(name + "UpdateProcessed called when it was already marked processed");
    }
  }

  @Override
  public Type getStatus(final String neighbor) {
    if (!update.get()) {
      LOG.warning(name + "Can't get status until all the updates are processed");
      return null;
    }
    LOG.info(name + "Getting status for " + neighbor);
    final Type status = neighborStatus.get(neighbor).getStatus();
    LOG.info(name + "Returning status " + status.toString() + " for " + neighbor);
    return status;
  }

  static class StatusProcessor {
    final Map<Type, Integer> typeCounts = new HashMap<Type, Integer>();
    private final String name;

    public StatusProcessor(final String name) {
      this.name = name;
    }

    public void addStatus(final Type status) {
      final int value = typeCounts.containsKey(status) ? typeCounts.get(status) : 0;
      typeCounts.put(status, value + 1);
    }

    public Type getStatus() {
      LOG.info(name + typeCounts.toString());
      if (typeCounts.size() > 2) {
        LOG.warning(name + "Each neighbor expects at most 2 types of status updates");
        return null;
      }
      final Set<Type> types = typeCounts.keySet();
      if (types.isEmpty()) {
        return null;
      }
      final Iterator<Type> typeIterator = types.iterator();
      final Type t1 = typeIterator.next();
      if (typeIterator.hasNext()) {
        final Type t2 = typeIterator.next();
        switch (t1) {
          case ParentAdd:
            if (!t2.equals(Type.ParentDead)) {
              LOG.warning(name + "Expect to find ParentDead as counter part to ParentAdd. Instead found" + t2);
              return null;
            }
            break;

          case ParentDead:
            if (!t2.equals(Type.ParentAdd)) {
              LOG.warning(name + "Expect to find ParentAdd as counter part to ParentDead. Instead found" + t2);
              return null;
            }
            break;

          case ChildAdd:
            if (!t2.equals(Type.ChildDead)) {
              LOG.warning(name + "Expect to find ChildDead as counter part to ChildAdd. Instead found" + t2);
              return null;
            }
            break;

          case ChildDead:
            if (!t2.equals(Type.ChildAdd)) {
              LOG.warning(name + "Expect to find ChildAdd as counter part to ChildDead. Instead found" + t2);
              return null;
            }
            break;

          default:
            LOG.warning(name + "Unexpected type in status");
            return null;
        }
        if (typeCounts.get(t1) == typeCounts.get(t2)) {
          return null;
        }
        return typeCounts.get(t1) > typeCounts.get(t2) ? t1 : t2;
      } else {
        return t1;
      }
    }
  }

  @Override
  public Iterator<String> iterator() {
    return neighborStatus.keySet().iterator();
  }

}
