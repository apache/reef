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
import org.apache.hadoop.mapred.Counters.Counter;
import org.apache.hadoop.mapred.*;
import org.apache.reef.annotations.audience.TaskSide;
import org.apache.reef.io.data.loading.api.DataSet;
import org.apache.reef.io.network.util.Pair;

import javax.inject.Inject;
import java.io.IOException;
import java.util.Iterator;

/**
 * An implementation of {@link DataSet} that reads records using a RecordReader
 * encoded inside an InputSplit.
 * <p/>
 * The input split is injected through an external constructor by deserializing
 * the input split assigned to this evaluator.
 *
 * @param <K>
 * @param <V>
 */
@TaskSide
public final class
    InputFormatDataSet<K extends WritableComparable<K>, V extends Writable>
    implements DataSet<K, V> {

  private final DummyReporter dummyReporter = new DummyReporter();
  private final JobConf jobConf;
  private final InputFormat<K, V> inputFormat;
  private final InputSplit split;
  private RecordReader lastRecordReader = null;

  @Inject
  public InputFormatDataSet(final InputSplit split, final JobConf jobConf) {
    this.jobConf = jobConf;
    this.inputFormat = this.jobConf.getInputFormat();
    this.split = split;
  }

  @Override
  public Iterator<Pair<K, V>> iterator() {
    try {

      final RecordReader newRecordReader =
          this.inputFormat.getRecordReader(this.split, this.jobConf, this.dummyReporter);

      if (newRecordReader == this.lastRecordReader) {
        throw new RuntimeException("Received the same record reader again. This isn't supported.");
      }

      this.lastRecordReader = newRecordReader;
      return new RecordReaderIterator(newRecordReader);

    } catch (final IOException ex) {
      throw new RuntimeException("Can't instantiate iterator.", ex);
    }
  }

  private final class RecordReaderIterator implements Iterator<Pair<K, V>> {

    private final RecordReader<K, V> recordReader;
    private Pair<K, V> recordPair;
    private boolean hasNext;

    RecordReaderIterator(final RecordReader<K, V> recordReader) {
      this.recordReader = recordReader;
      fetchRecord();
    }

    @Override
    public boolean hasNext() {
      return this.hasNext;
    }

    @Override
    public Pair<K, V> next() {
      final Pair<K, V> prevRecordPair = this.recordPair;
      fetchRecord();
      return prevRecordPair;
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException("Remove is not supported on RecordReader iterator");
    }

    private void fetchRecord() {
      this.recordPair = new Pair<>(this.recordReader.createKey(), this.recordReader.createValue());
      try {
        this.hasNext = this.recordReader.next(this.recordPair.first, this.recordPair.second);
      } catch (final IOException ex) {
        throw new RuntimeException("Unable to get InputSplits using the specified InputFormat", ex);
      }
    }
  }

  private final class DummyReporter implements Reporter {

    @Override
    public void progress() {
    }

    @Override
    public Counter getCounter(final Enum<?> key) {
      return null;
    }

    @Override
    public Counter getCounter(final String group, final String name) {
      return null;
    }

    @Override
    public InputSplit getInputSplit() throws UnsupportedOperationException {
      throw new UnsupportedOperationException("This is a Fake Reporter");
    }

    @Override
    public float getProgress() {
      return 0;
    }

    @Override
    public void incrCounter(final Enum<?> key, final long amount) {
    }

    @Override
    public void incrCounter(final String group, final String counter, final long amount) {
    }

    @Override
    public void setStatus(final String status) {
    }
  }
}
