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
package org.apache.reef.tests;

import org.apache.reef.tests.applications.ApplicationTestSuite;
import org.apache.reef.tests.close_eval.CloseEvaluatorTest;
import org.apache.reef.tests.configurationproviders.ConfigurationProviderTest;
import org.apache.reef.tests.driver.DriverTest;
import org.apache.reef.tests.runtimename.RuntimeNameTest;
import org.apache.reef.tests.evaluatorfailure.EvaluatorFailureTest;
import org.apache.reef.tests.evaluatorreuse.EvaluatorReuseTest;
import org.apache.reef.tests.evaluatorsize.EvaluatorSizeTest;
import org.apache.reef.tests.examples.ExamplesTestSuite;
import org.apache.reef.tests.fail.FailTestSuite;
import org.apache.reef.tests.files.FileResourceTest;
import org.apache.reef.tests.messaging.driver.DriverMessagingTest;
import org.apache.reef.tests.messaging.task.TaskMessagingTest;
import org.apache.reef.tests.statepassing.StatePassingTest;
import org.apache.reef.tests.subcontexts.SubContextTest;
import org.apache.reef.tests.taskresubmit.TaskResubmitTest;
import org.apache.reef.tests.watcher.WatcherTest;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * Test suite of all integration tests.
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
    DriverTest.class,
    EvaluatorReuseTest.class,
    EvaluatorSizeTest.class,
    FailTestSuite.class,
    FileResourceTest.class,
    DriverMessagingTest.class,
    TaskMessagingTest.class,
    StatePassingTest.class,
    SubContextTest.class,
    TaskResubmitTest.class,
    CloseEvaluatorTest.class,
    EvaluatorFailureTest.class,
    ExamplesTestSuite.class,
    ConfigurationProviderTest.class,
    ApplicationTestSuite.class,
    RuntimeNameTest.class,
    WatcherTest.class,
    })
public final class AllTestsSuite {
}
