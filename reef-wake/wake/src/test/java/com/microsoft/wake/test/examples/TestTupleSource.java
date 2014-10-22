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
package com.microsoft.wake.test.examples;

import org.junit.Test;

import com.microsoft.wake.examples.join.EventPrinter;
import com.microsoft.wake.examples.join.TupleEvent;
import com.microsoft.wake.examples.join.TupleSource;


public class TestTupleSource {
	@Test
	public void testOneThread() throws Exception {
		EventPrinter<TupleEvent> printer = new EventPrinter<>();
		TupleSource source = new TupleSource(printer, 256, 8, true);
		source.close();
	}
}
