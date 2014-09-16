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

package com.microsoft.reef.javabridge;

import com.microsoft.reef.driver.task.TaskMessage;

public class TaskMessageBridge extends NativeBridge{
    private TaskMessage jtaskMessage;
    private String taskId;

    // we don't really need to pass this around, just have this as place holder for future.
    public TaskMessageBridge(TaskMessage taskMessage)
    {
      jtaskMessage = taskMessage;
      taskId = taskMessage.getId();
    }

    @Override
    public void close()
    {
    }
}
