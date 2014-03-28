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

package javabridge;

import com.microsoft.reef.driver.evaluator.AllocatedEvaluator;

public class AllocatedEvaluatorBridge implements AutoCloseable {

    private AllocatedEvaluator jallocatedEvaluator;

    public AllocatedEvaluatorBridge(AllocatedEvaluator allocatedEvaluator)
    {
            jallocatedEvaluator = allocatedEvaluator;
    }

    public void submitContextAndTaskString(final String contextConfigurationString, final String taskConfigurationString)
    {
        // TODO: deserailze into configuration and use jallocatedEvaluator submitContextAndTask  instead
        // remove the  submitContextAndTaskString interface/impl from AllocatedEvaluator
        jallocatedEvaluator.submitContextAndTaskString(contextConfigurationString, taskConfigurationString);
    }

    @Override
    public void close()
    {
        jallocatedEvaluator.close();
    }

}
