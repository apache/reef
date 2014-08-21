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
package com.microsoft.reef.io.network.group.config;

import com.microsoft.reef.io.network.util.StringIdentifierFactory;
import com.microsoft.wake.ComparableIdentifier;
import com.microsoft.wake.IdentifierFactory;

import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

public class TaskTreeImpl implements TaskTree {
  private static final Logger LOG = Logger.getLogger(TaskTreeImpl.class.getName());

  private static class IdentifierStatus {

    public final ComparableIdentifier id;
    public Status status;

    @Override
    public String toString() {
      return "(" + this.id + ", " + this.status + ")";
    }

    @Override
    public boolean equals(final Object obj) {

      if (obj == null || !(obj instanceof IdentifierStatus)) {
        return false;
      }

      if (obj == this) {
        return true;
      }

      final IdentifierStatus that = (IdentifierStatus) obj;
      if (this.status != Status.ANY && that.status != Status.ANY) {
        return this.id.equals(that.id) && this.status.equals(that.status);
      } else {
        return this.id.equals(that.id);
      }
    }

    public IdentifierStatus(final ComparableIdentifier id) {
      super();
      this.id = id;
      this.status = Status.UNSCHEDULED;
    }

    public IdentifierStatus(final ComparableIdentifier id, final Status status) {
      super();
      this.id = id;
      this.status = status;
    }

    public static IdentifierStatus any(final ComparableIdentifier id) {
      return new IdentifierStatus(id, Status.ANY);
    }
  }

  private final List<IdentifierStatus> tasks = new ArrayList<>();
  private final TreeSet<Integer> holes = new TreeSet<>();

  @Override
  public synchronized void add(final ComparableIdentifier id) {
    LOG.log(Level.FINEST, "Before Tasks: {0}", this.tasks);
    // LOG.log(Level.FINEST, "Before Holes: " + holes);
    if (this.holes.isEmpty()) {
      // LOG.log(Level.FINEST, "No holes. Adding at the end");
      this.tasks.add(new IdentifierStatus(id));
    } else {
      // LOG.log(Level.FINEST, "Found holes: " + holes);
      final int firstHole = this.holes.first();
      // LOG.log(Level.FINEST, "First hole at " + firstHole + ". Removing it");
      this.holes.remove(firstHole);
      // LOG.log(Level.FINEST, "Adding " + id + " at " + firstHole);
      this.tasks.add(firstHole, new IdentifierStatus(id));
    }
    LOG.log(Level.FINEST, "After Tasks: {0}", this.tasks);
    // LOG.log(Level.FINEST, "After Holes: " + holes);
  }

  @Override
  public String toString() {
    return this.tasks.toString();
  }

  @Override
  public ComparableIdentifier parent(final ComparableIdentifier id) {
    final int idx = this.tasks.indexOf(IdentifierStatus.any(id));
    if (idx == -1 || idx == 0) {
      return null;
    }
    final int parIdx = (idx - 1) / 2;
    try {
      return this.tasks.get(parIdx).id;
    } catch (final IndexOutOfBoundsException e) {
      return null;
    }
  }

  @Override
  public ComparableIdentifier left(final ComparableIdentifier id) {
    final int idx = this.tasks.indexOf(IdentifierStatus.any(id));
    if (idx == -1) {
      return null;
    }
    final int leftIdx = idx * 2 + 1;
    try {
      return this.tasks.get(leftIdx).id;
    } catch (final IndexOutOfBoundsException e) {
      return null;
    }
  }

  @Override
  public ComparableIdentifier right(final ComparableIdentifier id) {
    final int idx = this.tasks.indexOf(IdentifierStatus.any(id));
    if (idx == -1) {
      return null;
    }
    final int rightIdx = idx * 2 + 2;
    try {
      return tasks.get(rightIdx).id;
    } catch (final IndexOutOfBoundsException e) {
      return null;
    }
  }

  @Override
  public List<ComparableIdentifier> neighbors(final ComparableIdentifier id) {
    final List<ComparableIdentifier> retVal = new ArrayList<>();
    final ComparableIdentifier parent = parent(id);
    if (parent != null) {
      retVal.add(parent);
    }
    retVal.addAll(children(id));
    return retVal;
  }

  @Override
  public List<ComparableIdentifier> children(final ComparableIdentifier id) {
    final List<ComparableIdentifier> retVal = new ArrayList<>();
    final ComparableIdentifier left = left(id);
    if (left != null) {
      retVal.add(left);
      final ComparableIdentifier right = right(id);
      if (right != null) {
        retVal.add(right);
      }
    }
    return retVal;
  }

  @Override
  public int childrenSupported(final ComparableIdentifier taskIdId) {
    return 2;
  }

  @Override
  public synchronized void remove(final ComparableIdentifier failedTaskId) {
    final int hole = this.tasks.indexOf(IdentifierStatus.any(failedTaskId));
    if (hole != -1) {
      this.holes.add(hole);
      this.tasks.remove(hole);
    }
  }

  @Override
  public List<ComparableIdentifier> scheduledChildren(final ComparableIdentifier taskId) {
    final List<ComparableIdentifier> children = children(taskId);
    final List<ComparableIdentifier> retVal = new ArrayList<>();
    for (final ComparableIdentifier child : children) {
      final Status s = getStatus(child);
      if (Status.SCHEDULED == s) {
        retVal.add(child);
      }
    }
    return retVal;
  }

  @Override
  public List<ComparableIdentifier> scheduledNeighbors(final ComparableIdentifier taskId) {
    final List<ComparableIdentifier> neighbors = neighbors(taskId);
    final List<ComparableIdentifier> retVal = new ArrayList<>();
    for (final ComparableIdentifier neighbor : neighbors) {
      final Status s = getStatus(neighbor);
      if (Status.SCHEDULED == s) {
        retVal.add(neighbor);
      }
    }
    return retVal;
  }

  @Override
  public void setStatus(final ComparableIdentifier taskId, final Status status) {
    final int idx = this.tasks.indexOf(IdentifierStatus.any(taskId));
    if (idx != -1) {
      this.tasks.get(idx).status = status;
    }
  }

  @Override
  public Status getStatus(final ComparableIdentifier taskId) {
    final int idx = this.tasks.indexOf(IdentifierStatus.any(taskId));
    return idx == -1 ? null : this.tasks.get(idx).status;
  }

  public static void main(final String[] args) {

    final IdentifierFactory idFac = new StringIdentifierFactory();
    final TaskTree tree = new TaskTreeImpl();
    final ComparableIdentifier[] ids = new ComparableIdentifier[7];
    for (int i = 0; i < ids.length; ++i) {
      ids[i] = (ComparableIdentifier) idFac.getNewInstance(Integer.toString(i));
    }

    tree.add(ids[0]);
    tree.add(ids[2]);
    tree.add(ids[1]);
    tree.add(ids[6]);
    tree.add(ids[4]);
    tree.add(ids[5]);
    //tree.add(ids[3]);

    tree.setStatus(ids[2], Status.SCHEDULED);
    LOG.log(Level.FINEST, "Tree: {0}", tree);
    LOG.log(Level.FINEST, "Children:\n{0}", tree.children(ids[0]));
    LOG.log(Level.FINEST, "Scheduled Children:\n{0}", tree.scheduledChildren(ids[0]));

    final Queue<ComparableIdentifier> idss = new LinkedList<>();
    idss.add((ComparableIdentifier) idFac.getNewInstance("0"));
    while (!idss.isEmpty()) {
      final ComparableIdentifier id = idss.poll();
      LOG.log(Level.FINEST, "Id: {0}", id);
      final ComparableIdentifier left = tree.left(id);
      if (left != null) {
        idss.add(left);
        final ComparableIdentifier right = tree.right(id);
        if (right != null) {
          idss.add(right);
        }
      }
    }
    tree.setStatus(ids[4], Status.SCHEDULED);
    LOG.log(Level.FINEST, "Tree: {0}", tree);
    LOG.log(Level.FINEST, "Neighbors:\n{0}", tree.neighbors(ids[2]));
    LOG.log(Level.FINEST, "Scheduled Neighbors:\n{0}", tree.scheduledNeighbors(ids[2]));
  }
}
