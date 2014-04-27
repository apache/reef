/*
 * Copyright 2013 Microsoft.
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
package com.microsoft.reef.io.data.loading.impl;

import java.io.IOException;
import java.util.Iterator;

import javax.inject.Inject;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.Counters.Counter;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

import com.microsoft.reef.annotations.audience.TaskSide;
import com.microsoft.reef.io.data.loading.api.DataSet;
import com.microsoft.reef.io.network.util.Utils.Pair;
import com.microsoft.tang.annotations.Parameter;

/**
 * An implementation of {@link DataSet} that reads
 * records using a RecordReader encoded inside an
 * InputSplit.
 * 
 * The input split is injected through an external
 * constructor by deserializing the input split
 * assigned to this evaluator
 * 
 * @param <K>
 * @param <V>
 */
@TaskSide
public class InputFormatDataSet<K extends WritableComparable<K>,V extends Writable> implements DataSet<K,V> {
  private final RecordReader<K, V> recordReader;

  @Inject
  public InputFormatDataSet(
        InputSplit split,
        @Parameter(InputFormatExternalConstructor.SerializedJobConf.class) String serializedJobConf
      ) {
    final JobConf jobConf = WritableSerializer.deserialize(serializedJobConf);
    final FakeReporter fakeReporter = new FakeReporter();
    try {
      this.recordReader = jobConf.getInputFormat().getRecordReader(split, jobConf, fakeReporter);
    } catch (IOException e) {
      throw new RuntimeException("Unable to get InputSplits using the specified InputFormat", e);
    }
  }

  @Override
  public Iterator<Pair<K,V>> iterator() {
    return new RecordReaderIterator(this.recordReader);
  }
  
  class RecordReaderIterator implements Iterator<Pair<K,V>>{
    
    private final RecordReader<K,V> recordReader;
    private K key;
    private V value;
    private boolean available;
    
    public RecordReaderIterator(final RecordReader<K,V> recordReader) {
      this.recordReader = recordReader;
      fetchRecord();
    }

    @Override
    public boolean hasNext() {
      return available;
    }

    @Override
    public Pair<K, V> next() {
      fetchRecord();
      return new Pair<K, V>(key, value);
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException("Remove is not supported on RecordReader iterator");
    }

    private void fetchRecord() {
      key = this.recordReader.createKey();
      value = this.recordReader.createValue();
      try {
        available = this.recordReader.next(key, value);
      } catch (IOException e) {
        throw new RuntimeException("Unable to get InputSplits using the specified InputFormat", e);
      }
    }
  }

  class FakeReporter implements Reporter{

    @Override
    public void progress() {    }

    @Override
    public Counter getCounter(Enum<?> arg0) {
      return null;
    }

    @Override
    public Counter getCounter(String arg0, String arg1) {
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
    public void incrCounter(Enum<?> arg0, long arg1) {    }

    @Override
    public void incrCounter(String arg0, String arg1, long arg2) {    }

    @Override
    public void setStatus(String arg0) {    }
  }
}
