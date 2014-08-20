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
package com.microsoft.reef.io.network.group.impl.operators.faulty;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

import javax.inject.Inject;

import com.microsoft.wake.EventHandler;

public class ExceptionHandler implements EventHandler<Exception> {
  private static final Logger logger = Logger.getLogger(ExceptionHandler.class.getName());
  List<Exception> exceptions = new ArrayList<>();
  
  @Inject
  public ExceptionHandler() {  }

  @Override
  public synchronized void onNext(Exception arg0) {
    exceptions.add(arg0);
    logger.fine("Got an exception. Added it to list(" + exceptions.size() + ")");
  }
  
  public synchronized boolean hasExceptions(){
    boolean ret = !exceptions.isEmpty();
    logger.fine("There are " + exceptions.size() + " exceptions. Clearing now");
    exceptions.clear();
    return ret;
  }

}
