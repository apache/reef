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
package org.apache.reef.io.watcher;

import org.apache.reef.annotations.Unstable;
import org.apache.reef.io.watcher.util.RunnableExecutingHandler;
import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.wake.EStage;
import org.apache.reef.wake.impl.ThreadPoolStage;

import javax.inject.Inject;
import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.nio.charset.Charset;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Write events to a file in the root directory of the driver.
 */
@Unstable
public final class FileEventStream implements EventStream {

  private final DateFormat dateFormat;
  private final PrintWriter printWriter;
  private final EStage<Runnable> singleThreadedExecutor;

  @Inject
  private FileEventStream(@Parameter(Path.class) final String path) {
    this.dateFormat = new SimpleDateFormat("[yyyy.MM.dd HH:mm:ss.SSSS]");
    this.singleThreadedExecutor = new ThreadPoolStage<>(new RunnableExecutingHandler(), 1);

    try {
      final OutputStreamWriter writer = new OutputStreamWriter(
          new FileOutputStream(createFileWithPath(path)), Charset.forName("UTF-8"));
      this.printWriter = new PrintWriter(writer);
    } catch (final Exception e) {
      throw new RuntimeException(e);
    }
  }

  private File createFileWithPath(final String path) throws Exception {
    final File file = new File(path);
    final File parent = file.getParentFile();
    if (parent != null && !parent.exists()){
      parent.mkdirs();
    }

    file.createNewFile();
    return file;
  }

  @Override
  public void onEvent(final EventType type, final String jsonEncodedEvent) {
    final long timestamp = System.currentTimeMillis();
    singleThreadedExecutor.onNext(new Runnable() {
      @Override
      public void run() {
        final String eventDescription = new StringBuilder()
            .append(dateFormat.format(new Date(timestamp)))
            .append(" [")
            .append(type)
            .append("] ")
            .append(jsonEncodedEvent)
            .toString();

        printWriter.println(eventDescription);

        if (type == EventType.RuntimeStop) {
          onRuntimeStop();
        }
      }
    });
  }

  private void onRuntimeStop() {
    printWriter.flush();
    printWriter.close();
  }

  @NamedParameter(doc = "The relative path of the reporting file.", default_value = "watcher_report.txt")
  public static final class Path implements Name<String> {
  }
}
