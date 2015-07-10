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
package org.apache.reef.io.data.loading.impl;

import org.apache.commons.codec.binary.Base64;
import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;

import java.io.*;
import java.util.Set;

/**
 * Serialize and deserialize {@link DistributedDataSetPartition} objects.
 */
public final class DistributedDataSetPartitionSerializer {

  public static String serialize(final DistributedDataSetPartition partition) {
    try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
      final DataOutputStream daos = new DataOutputStream(baos);
      daos.writeUTF(partition.getPath());
      daos.writeUTF(partition.getLocation());
      daos.writeInt(partition.getDesiredSplits());
      return Base64.encodeBase64String(baos.toByteArray());
    } catch (final IOException e) {
      throw new RuntimeException("Unable to serialize distributed data partition", e);
    }
  }

  public static DistributedDataSetPartition deserialize(final String serializedPartition) {
    try (ByteArrayInputStream bais = new ByteArrayInputStream(Base64.decodeBase64(serializedPartition))) {
      final DataInputStream dais = new DataInputStream(bais);
      return new DistributedDataSetPartition(dais.readUTF(), dais.readUTF(), dais.readInt());
    } catch (final IOException e) {
      throw new RuntimeException("Unable to de-serialize distributed data partition", e);
    }
  }

  /**
   * Empty private constructor to prohibit instantiation of utility class.
   */
  private DistributedDataSetPartitionSerializer() {
  }

  /**
   * Allows to specify a set of distributed data set partitions.
   */
  @NamedParameter(doc = "Sets of distributed data set partitions")
  public static final class DistributedDataSetPartitions implements Name<Set<String>> {
  }
}
