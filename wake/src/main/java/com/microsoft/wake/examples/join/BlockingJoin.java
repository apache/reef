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

import com.microsoft.wake.rx.Observer;
import com.microsoft.wake.rx.StaticObservable;


public class BlockingJoin implements StaticObservable {
	private final Observer<TupleEvent> out;
	private final ConcurrentSkipListSet<TupleEvent> left = new ConcurrentSkipListSet<>();
	boolean leftDone = false;

	private synchronized void tellEveryoneLeftIsDone() {
		leftDone = true;
		notifyAll();
	}

	private synchronized void waitUntilLeftIsDone() {
		while(!leftDone) {
			try {
				wait();
			} catch (InterruptedException e) {
				throw new IllegalStateException(
						"No support for interrupted threads here!", e);
			}
		}
	}

	public BlockingJoin(Observer<TupleEvent> out) {
		this.out = out;
	}

	public Observer<TupleEvent> wireLeft() {
		return new Observer<TupleEvent>() {

			@Override
			public void onNext(TupleEvent value) {
				left.add(value);
			}

			@Override
			public void onError(Exception error) {

			}

			@Override
			public void onCompleted() {
				tellEveryoneLeftIsDone();
			}

		};
	}
	public Observer<TupleEvent> wireRight() {
		return new Observer<TupleEvent>() {

			@Override
			public void onNext(TupleEvent value) {
				if(!leftDone) waitUntilLeftIsDone();
				if(left.contains(value)) {
					out.onNext(value);
				}
			}

			@Override
			public void onError(Exception error) {
			}

			@Override
			public void onCompleted() {
				waitUntilLeftIsDone();
				out.onCompleted();
			}
		};
	}
}
