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
package com.microsoft.wake.time;

/**
 * Time object
 */
public abstract class Time implements Comparable<Time> {

	private final long timestamp;
	
	public Time(final long timestamp) {
		this.timestamp = timestamp;
	}
	
	public final long getTimeStamp() {
		return this.timestamp;
	}
	
	@Override
	public final String toString() {
		return this.getClass().getName() + "[" + this.timestamp + "]";
	}
	
	@Override
	public final int compareTo(Time o) {
		if (this.timestamp < o.timestamp) return -1;
		if (this.timestamp > o.timestamp) return 1;
		if (this.hashCode() < o.hashCode()) return -1;
		if (this.hashCode() > o.hashCode()) return 1;
		return 0;
	}
	
	@Override
	public final boolean equals(Object o) {
		if (o instanceof Time) {
			return compareTo((Time)o) == 0;
		}
		return false;
	}
	
	@Override
	public final int hashCode() {
		return super.hashCode();
	}
}