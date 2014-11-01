/**
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

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.reef.io.data.loading.api.DataSet;
import org.apache.reef.io.network.util.Pair;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class InMemoryInputFormatDataSet<K extends WritableComparable<K>, V extends Writable>
    implements DataSet<K, V> {

  private final InputFormatDataSet<K, V> inputFormatDataSet;
  private List<Pair<K, V>> recordsList = null;

  @Inject
  public InMemoryInputFormatDataSet(InputFormatDataSet<K, V> inputFormatDataSet) {
    this.inputFormatDataSet = inputFormatDataSet;
  }


  @Override
  public synchronized Iterator<Pair<K, V>> iterator() {
    if (recordsList == null) {
      recordsList = new ArrayList<>();
      for (final Pair<K, V> keyValue : inputFormatDataSet) {
        recordsList.add(keyValue);
      }
    }
    return recordsList.iterator();
  }
}
