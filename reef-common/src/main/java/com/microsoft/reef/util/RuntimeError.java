/**
 * Copyright (C) 2013 Microsoft Corporation
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
package com.microsoft.reef.util;

import com.microsoft.reef.proto.ReefServiceProtos.RuntimeErrorProto;
import com.microsoft.wake.remote.impl.ObjectSerializableCodec;

public class RuntimeError {

	private final RuntimeErrorProto error;
	
	public RuntimeError(final RuntimeErrorProto error) {
		this.error = error;
	}
	
	public final String toString() {
		return this.error.getMessage();
	}
	
	public final String getRuntimeName() {
		return this.error.getName();
	}

  public final boolean hasIdentifier() {
    return this.error.hasIdentifier();
  }

  public final String getIdentifier() {
    return this.error.getIdentifier();
  }

	public final boolean hasException() {
		return this.error.hasException();
	}

	public final Exception getException() {
		if (this.error.hasException()) {
			final ObjectSerializableCodec<Exception> codec = new ObjectSerializableCodec<>();
			return codec.decode(this.error.getException().toByteArray());
		}
		return null;
	}
}
