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

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.reef.io.checkpoint.CheckpointID;
import org.apache.reef.io.checkpoint.CheckpointNamingService;
import org.apache.reef.io.checkpoint.CheckpointService;
import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;

/**
 * A FileSystem based CheckpointService.
 */
public class FSCheckpointService implements CheckpointService {

  private final Path base;
  private final FileSystem fs;
  private final CheckpointNamingService namingPolicy;
  private final short replication;

  @Inject
  FSCheckpointService(final FileSystem fs,
                      @Parameter(PATH.class) final String basePath,
                      final CheckpointNamingService namingPolicy,
                      @Parameter(ReplicationFactor.class) final short replication) {
    this.fs = fs;
    this.base = new Path(basePath);
    this.namingPolicy = namingPolicy;
    this.replication = replication;
  }

  public FSCheckpointService(final FileSystem fs,
                             final Path base,
                             final CheckpointNamingService namingPolicy,
                             final short replication) {
    this.fs = fs;
    this.base = base;
    this.namingPolicy = namingPolicy;
    this.replication = replication;
  }

  static final Path tmpfile(final Path p) {
    return new Path(p.getParent(), p.getName() + ".tmp");
  }

  public CheckpointWriteChannel create()
      throws IOException {

    final String name = namingPolicy.getNewName();

    final Path p = new Path(name);
    if (p.isUriPathAbsolute()) {
      throw new IOException("Checkpoint cannot be an absolute path");
    }
    return createInternal(new Path(base, p));
  }

  CheckpointWriteChannel createInternal(final Path name) throws IOException {

    //create a temp file, fail if file exists
    return new FSCheckpointWriteChannel(name, fs.create(tmpfile(name), replication));
  }

  @Override
  public CheckpointReadChannel open(final CheckpointID id)
      throws IOException, InterruptedException {
    if (!(id instanceof FSCheckpointID)) {
      throw new IllegalArgumentException(
          "Mismatched checkpoint type: " + id.getClass());
    }
    return new FSCheckpointReadChannel(
        fs.open(((FSCheckpointID) id).getPath()));
  }

  @Override
  public CheckpointID commit(final CheckpointWriteChannel ch) throws IOException,
      InterruptedException {
    if (ch.isOpen()) {
      ch.close();
    }
    final FSCheckpointWriteChannel hch = (FSCheckpointWriteChannel) ch;
    final Path dst = hch.getDestination();
    if (!fs.rename(tmpfile(dst), dst)) {
      // attempt to clean up
      abort(ch);
      throw new IOException("Failed to promote checkpoint" +
          tmpfile(dst) + " -> " + dst);
    }
    return new FSCheckpointID(hch.getDestination());
  }

  @Override
  public void abort(final CheckpointWriteChannel ch) throws IOException {
    if (ch.isOpen()) {
      ch.close();
    }
    final FSCheckpointWriteChannel hch = (FSCheckpointWriteChannel) ch;
    final Path tmp = tmpfile(hch.getDestination());
    try {
      if (!fs.delete(tmp, false)) {
        throw new IOException("Failed to delete checkpoint during abort");
      }
    } catch (final FileNotFoundException ignored) {
      // IGNORE
    }
  }

  @Override
  public boolean delete(final CheckpointID id) throws IOException,
      InterruptedException {
    if (!(id instanceof FSCheckpointID)) {
      throw new IllegalArgumentException(
          "Mismatched checkpoint type: " + id.getClass());
    }
    final Path tmp = ((FSCheckpointID) id).getPath();
    try {
      return fs.delete(tmp, false);
    } catch (final FileNotFoundException ignored) {
      // IGNORE
    }
    return true;
  }

  @NamedParameter(doc = "The path to be used to store the checkpoints.")
  static class PATH implements Name<String> {
  }

  @NamedParameter(doc = "The replication factor to be used for the stored checkpoints", default_value = "3")
  static class ReplicationFactor implements Name<Short> {
  }

  private static class FSCheckpointWriteChannel
      implements CheckpointWriteChannel {
    private final Path finalDst;
    private final WritableByteChannel out;
    private boolean isOpen = true;

    FSCheckpointWriteChannel(final Path finalDst, final FSDataOutputStream out) {
      this.finalDst = finalDst;
      this.out = Channels.newChannel(out);
    }

    public int write(final ByteBuffer b) throws IOException {
      return out.write(b);
    }

    public Path getDestination() {
      return finalDst;
    }

    @Override
    public void close() throws IOException {
      isOpen = false;
      out.close();
    }

    @Override
    public boolean isOpen() {
      return isOpen;
    }

  }

  private static class FSCheckpointReadChannel
      implements CheckpointReadChannel {

    private final ReadableByteChannel in;
    private boolean isOpen = true;

    FSCheckpointReadChannel(final FSDataInputStream in) {
      this.in = Channels.newChannel(in);
    }

    @Override
    public int read(final ByteBuffer bb) throws IOException {
      return in.read(bb);
    }

    @Override
    public void close() throws IOException {
      isOpen = false;
      in.close();
    }

    @Override
    public boolean isOpen() {
      return isOpen;
    }

  }

}
