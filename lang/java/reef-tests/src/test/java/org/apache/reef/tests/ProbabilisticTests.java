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
package org.apache.reef.tests;

import org.apache.reef.tests.taskcounting.TaskCountingTest;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * A test suite for probabilistic tests: Tests that run man, many times in order to check for HeisenBugs.
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
    TaskCountingTest.class
})
public final class ProbabilisticTests {
}
