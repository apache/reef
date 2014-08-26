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
package com.microsoft.reef.tests;

import com.microsoft.reef.tests.close_eval.CloseEvaluatorTest;
import com.microsoft.reef.tests.driver.DriverTest;
import com.microsoft.reef.tests.evaluatorfailure.EvaluatorFailureTest;
import com.microsoft.reef.tests.evaluatorreuse.EvaluatorReuseTest;
import com.microsoft.reef.tests.evaluatorsize.EvaluatorSizeTest;
import com.microsoft.reef.tests.examples.ExamplesTestSuite;
import com.microsoft.reef.tests.fail.FailTestSuite;
import com.microsoft.reef.tests.files.FileResourceTest;
import com.microsoft.reef.tests.messaging.driver.DriverMessagingTest;
import com.microsoft.reef.tests.messaging.task.TaskMessagingTest;
import com.microsoft.reef.tests.statepassing.StatePassingTest;
import com.microsoft.reef.tests.subcontexts.SubContextTest;
import com.microsoft.reef.tests.taskresubmit.TaskResubmitTest;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

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
    ExamplesTestSuite.class
})
public final class AllTestsSuite {
}
