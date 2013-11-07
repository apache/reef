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

import com.microsoft.reef.annotations.audience.DriverSide;
import com.microsoft.reef.driver.activity.RunningActivity;
import com.microsoft.reef.io.naming.Identifiable;

import java.util.concurrent.ExecutionException;

/**
 * Exception thrown to the Driver when an Evaluator becomes unusable.
 *
 */
@DriverSide
public class EvaluatorException extends ExecutionException implements Identifiable {

    private static final long serialVersionUID = 1L;
    private final transient String evaluatorID;
    private final transient RunningActivity runningActivity;

    public EvaluatorException(final String evaluatorID) {
        super();
        this.evaluatorID = evaluatorID;
        this.runningActivity = null;
    }

    public EvaluatorException(final String evaluatorID, final String message, final Throwable cause) {
        super(message, cause);
        this.evaluatorID = evaluatorID;
        this.runningActivity = null;
    }

    public EvaluatorException(final String evaluatorID, final String message) {
    	this(evaluatorID, message, (RunningActivity) null);
    }
    
    public EvaluatorException(final String evaluatorID, final String message, final RunningActivity runningActivity) {
        super(message);
        this.evaluatorID = evaluatorID;
        this.runningActivity = runningActivity;
    }

    public EvaluatorException(final String evaluatorID, final Throwable cause) {
    	this(evaluatorID, cause, null);
    }
    
    public EvaluatorException(final String evaluatorID, final Throwable cause, final RunningActivity runningActivity) {
        super(cause);
        this.evaluatorID = evaluatorID;
        this.runningActivity = runningActivity;
    }

    /**
     * Access the affected Evaluator.
     *
     * @return the affected Evaluator.
     */
	@Override
	public String getId() {
        return this.evaluatorID;
	}
	
	public final RunningActivity getRunningActivity() {
		return this.runningActivity;
	}
}