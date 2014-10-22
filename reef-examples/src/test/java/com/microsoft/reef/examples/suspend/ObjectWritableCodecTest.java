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
package com.microsoft.reef.examples.suspend;

import com.microsoft.reef.io.checkpoint.CheckpointID;
import com.microsoft.reef.io.checkpoint.fs.FSCheckpointID;
import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class ObjectWritableCodecTest {

  private static ObjectWritableCodec<CheckpointID> codec;

  /**
   * Test class setup - create the codec.
   */
  @BeforeClass
  public static void setUpClass() {
    codec = new ObjectWritableCodec<CheckpointID>(FSCheckpointID.class);
  }

  /**
   * After the encode/decode cycle result equals to the original object.
   */
  @Test
  public void testFSCheckpointIdCodec() {
    final CheckpointID checkpoint1 = new FSCheckpointID(new Path("path"));
    final byte[] serialized = codec.encode(checkpoint1);
    final CheckpointID checkpoint2 = codec.decode(serialized);
    Assert.assertEquals(checkpoint1, checkpoint2);
  }
}
