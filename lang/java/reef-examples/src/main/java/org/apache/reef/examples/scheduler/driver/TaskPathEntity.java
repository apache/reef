/*
 * Copyright 2015 The Apache Software Foundation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.reef.examples.scheduler.driver;

/**
 * TaskPathEntity represent a single Task plus path entry in a task queue used
 * in the scheduler.
 */
final class TaskPathEntity {

  private final TaskEntity t;
  private final String p;

  public TaskPathEntity(final TaskEntity t, final String p) {
    this.t = t;
    this.p = p;
  }

  public TaskEntity getTask() {
    return t;
  }

  public String getPath() {
    return p;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    final TaskPathEntity that = (TaskPathEntity) o;

    if (p.equals(that.getPath())) {
      return false;
    }

    if (!t.equals(that.getTask())) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    int result = p.hashCode();
    result = 31 * result + t.hashCode();
    return result;
  }

}
