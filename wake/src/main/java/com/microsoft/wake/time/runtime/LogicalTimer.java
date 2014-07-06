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
package com.microsoft.wake.time.runtime;

import javax.inject.Inject;

public final class LogicalTimer implements Timer {

	private long current = 0;
	
	@Inject
	LogicalTimer() {}
	
	@Override
	public long getCurrent() {
		return this.current;
	}

	@Override
	public long getDuration(long time) {
	    isReady(time);
		return 0;
	}

	@Override
	public boolean isReady(long time) {
		if (this.current < time) this.current = time;
		return true;
	}

}
