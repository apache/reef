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
package com.microsoft.reef.io.storage.local;

import com.microsoft.reef.io.storage.StorageService;



public class LocalStorageService implements StorageService {
	@SuppressWarnings("unused")
	private final String jobName;
	@SuppressWarnings("unused")
	private final String evaluatorName;
	
	private final LocalScratchSpace scratchSpace;
	
	public LocalStorageService(String jobName, String evaluatorName) {
		this.jobName = jobName;
		this.evaluatorName = evaluatorName;
		this.scratchSpace = new LocalScratchSpace(jobName,evaluatorName);
	}
	
	@Override
	public LocalScratchSpace getScratchSpace() {
		return scratchSpace;
	};
}
