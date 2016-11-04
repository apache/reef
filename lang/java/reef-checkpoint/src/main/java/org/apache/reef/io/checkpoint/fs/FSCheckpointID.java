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
package org.apache.reef.io.checkpoint.fs;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.reef.io.checkpoint.CheckpointID;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * A FileSystem based checkpoint ID contains reference to the Path
 * where the checkpoint has been saved.
 */
public class FSCheckpointID implements CheckpointID {

  private Path path;

  // CheckpointID extends Hadoop Writable interface that enables serialization
  // Java serialization requires a (potentially empty) public default constructor
  public FSCheckpointID(){}

  public FSCheckpointID(final Path path) {
    this.path = path;
  }

  public Path getPath() {
    return path;
  }

  @Override
  public String toString() {
    return path.toString();
  }

  @Override
  public void write(final DataOutput out) throws IOException {
    Text.writeString(out, path.toString());
  }

  @Override
  public void readFields(final DataInput in) throws IOException {
    this.path = new Path(Text.readString(in));
  }

  @Override
  public boolean equals(final Object other) {
    return (other instanceof FSCheckpointID)
            && path.equals(((FSCheckpointID) other).path);
  }

  @Override
  public int hashCode() {
    return path.hashCode();
  }

}
