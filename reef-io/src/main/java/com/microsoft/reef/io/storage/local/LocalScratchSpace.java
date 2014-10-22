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

import com.microsoft.reef.io.storage.ScratchSpace;

import java.io.File;
import java.io.IOException;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;

public class LocalScratchSpace implements ScratchSpace {

	private final String jobName;
	private final String evaluatorName;
	/** Zero denotes "unlimited" */
	private long quota;
	private final Set<File> tempFiles = new ConcurrentSkipListSet<File>();

	public LocalScratchSpace(String jobName, String evaluatorName) {
		this.jobName = jobName;
		this.evaluatorName = evaluatorName;
		this.quota = 0;
	}

	public LocalScratchSpace(String jobName, String evaluatorName, long quota) {
		this.jobName = jobName;
		this.evaluatorName = evaluatorName;
		this.quota = quota;
	}

	public File newFile() {
		final File ret;
		try {
			ret = File.createTempFile("reef-" + jobName + "-" + evaluatorName,
					"tmp");
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		tempFiles.add(ret);
		return ret;
	}

	@Override
	public long availableSpace() {
		return quota;
	}

	@Override
	public long usedSpace() {
		long ret = 0;
		for (File f : tempFiles) {
			// TODO: Error handling...
			ret += f.length();
		}
		return ret;
	}

	@Override
	public void delete() {
		// TODO: Error handling. Files.delete() would give us an exception. We
		// should pass a set of Exceptions into a ReefRuntimeException.
		for (File f : tempFiles) {
			f.delete();
		}
		tempFiles.clear();
	}

}
