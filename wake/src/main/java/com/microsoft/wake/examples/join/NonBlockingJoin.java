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
package com.microsoft.wake.examples.join;

import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicBoolean;

import com.microsoft.wake.rx.Observer;
import com.microsoft.wake.rx.StaticObservable;


public class NonBlockingJoin implements StaticObservable {
	private final AtomicBoolean leftDone = new AtomicBoolean(false);
	private final AtomicBoolean completed = new AtomicBoolean(false);
	private final AtomicBoolean sentCompleted = new AtomicBoolean(false);

	private final Observer<TupleEvent> out;

	private final ConcurrentSkipListSet<TupleEvent> leftTable = new ConcurrentSkipListSet<TupleEvent>();
	private final ConcurrentSkipListSet<TupleEvent> rightTable = new ConcurrentSkipListSet<TupleEvent>();

	private void drainRight() {
		TupleEvent t;
		if (leftDone.get()) {
			while ((t = rightTable.pollFirst()) != null) {
				if (leftTable.contains(t)) {
					out.onNext(t);
				}
			}
			if (completed.get()) {
				// There cannot be any more additions to rightTable after
				// completed is set to true, so this ensures that rightTable is
				// really empty. (Someone could have inserted into it during the
				// race between the previous while loop and the check of
				// completed.)
				while ((t = rightTable.pollFirst()) != null) {
					if (leftTable.contains(t)) {
						out.onNext(t);
					}
				}
				if (sentCompleted.getAndSet(true) == false) {
					out.onCompleted();
				}
			}
		}
	}

	public NonBlockingJoin(Observer<TupleEvent> out) {
		this.out = out;
	}

	public Observer<TupleEvent> wireLeft() {
		return new Observer<TupleEvent>() {

			@Override
			public void onNext(TupleEvent value) {
				leftTable.add(value);
			}

			@Override
			public void onError(Exception error) {
				leftTable.clear();
				rightTable.clear();
				out.onError(error);
			}

			@Override
			public void onCompleted() {
				leftDone.set(true);
				drainRight();
			}

		};
	}

	public Observer<TupleEvent> wireRight() {
		return new Observer<TupleEvent>() {

			@Override
			public void onNext(TupleEvent value) {
				if (leftTable.contains(value)) {
					out.onNext(value);
				} else if (!leftDone.get()) {
					rightTable.add(value);
				}
				drainRight();
			}

			@Override
			public void onError(Exception error) {
				leftTable.clear();
				rightTable.clear();
				out.onError(error);
			}

			@Override
			public void onCompleted() {
				completed.set(true);
				drainRight();
			}
		};

	}

}
