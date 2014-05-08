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

import com.microsoft.reef.driver.context.ActiveContext;
import com.microsoft.tang.ClassHierarchy;
import com.microsoft.tang.Configuration;
import com.microsoft.tang.formats.AvroConfigurationSerializer;

import java.util.logging.Level;
import java.util.logging.Logger;

public class ActiveContextBridge implements AutoCloseable {
    private static final Logger LOG = Logger.getLogger(ActiveContextBridge.class.getName());

    private ActiveContext jactiveContext;

    private AvroConfigurationSerializer serializer;

    public ActiveContextBridge(ActiveContext activeContext)
    {
        jactiveContext = activeContext;
        serializer = new AvroConfigurationSerializer();
    }

    public void submitTaskString( final String taskConfigurationString)
    {

        if(taskConfigurationString.isEmpty())
        {
            throw new RuntimeException("empty taskConfigurationString provided.");
        }
        ClassHierarchy clrClassHierarchy = Utilities.loadClassHierarchy(NativeInterop.CLASS_HIERARCHY_FILENAME);
        Configuration taskConfiguration;
        try {
            taskConfiguration = serializer.fromString(taskConfigurationString, clrClassHierarchy);
        } catch (final Exception e) {
            final String message = "Unable to de-serialize CLR  task configurations using class hierarchy.";
            LOG.log(Level.SEVERE, message, e);
            throw new RuntimeException(message, e);
        }
        jactiveContext.submitTask(taskConfiguration);
    }

    @Override
    public void close()
    {
        jactiveContext.close();
    }
}
