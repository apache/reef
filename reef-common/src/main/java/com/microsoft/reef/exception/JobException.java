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
package com.microsoft.reef.exception;

import com.microsoft.reef.io.naming.Identifiable;

import java.util.concurrent.ExecutionException;

/**
 * Exception thrown to the Driver if a Job needed to be terminated.
 */
public class JobException extends ExecutionException implements Identifiable {

    private static final long serialVersionUID = 1L;
    private final transient String jobid;

    /**
     * Standard Exception constructor.
     *
     */
    public JobException(final String jobid) {
    	super();
        this.jobid = jobid;
    }

    /**
     * Standard Exception constructor.
     *
     * @param jobid the job identifier
     * @param message
     * @param cause
     */
    public JobException(final String jobid, final String message, final Throwable cause) {
        super(message, cause);
        this.jobid = jobid;
    }

    /**
     * Standard Exception constructor.
     *
     * @param jobid the job identifier
     * @param message
     */
    public JobException(final String jobid, final String message) {
        super(message);
        this.jobid = jobid;
    }

    /**
     * Standard Exception constructor.
     *
     * @param evaluator the Evaluator in which the problem happened.
     * @param cause
     */
    public JobException(final String jobid, final Throwable cause) {
        super(cause);
        this.jobid = jobid;
    }

    /**
     * 
     * @return the job identifier
     */
    public final String getId() {
    	return this.jobid;
    }
}
