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
import com.microsoft.tang.ClassHierarchy;
import com.microsoft.tang.Configuration;
import com.microsoft.tang.exceptions.BindException;
import com.microsoft.tang.formats.AvroConfigurationSerializer;
import com.microsoft.tang.implementation.protobuf.ProtocolBufferClassHierarchy;
import com.microsoft.tang.proto.ClassHierarchyProto;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.logging.Level;
import java.util.logging.Logger;

public class AllocatedEvaluatorBridge implements AutoCloseable {

    private static final Logger LOG = Logger.getLogger(AllocatedEvaluatorBridge.class.getName());

    private AllocatedEvaluator jallocatedEvaluator;

    private  AvroConfigurationSerializer serializer;


    public static final String CLASS_HIERARCHY_FILENAME = "clrClassHierarchy.bin";

    public AllocatedEvaluatorBridge(AllocatedEvaluator allocatedEvaluator)
    {
            jallocatedEvaluator = allocatedEvaluator;
            serializer = new AvroConfigurationSerializer();
    }

    public void submitContextAndTaskString(final String contextConfigurationString, final String taskConfigurationString)
    {
        // TODO: deserailze into configuration and use jallocatedEvaluator submitContextAndTask  instead
        // remove the  submitContextAndTaskString interface/impl from AllocatedEvaluator
        if(contextConfigurationString.isEmpty())
        {
            throw new RuntimeException("empty contextConfigurationString provided.");
        }
        if(taskConfigurationString.isEmpty())
        {
            throw new RuntimeException("empty taskConfigurationString provided.");
        }
        ClassHierarchy clrClassHierarchy = loadClassHierarchy(CLASS_HIERARCHY_FILENAME);
        Configuration contextConfiguration;
        Configuration taskConfiguration;
        try (final InputStream chin = new FileInputStream(CLASS_HIERARCHY_FILENAME)) {
            contextConfiguration = serializer.fromString(contextConfigurationString, clrClassHierarchy);
            taskConfiguration = serializer.fromString(taskConfigurationString, clrClassHierarchy);
        } catch (final Exception e) {
            final String message = "Unable to de-serialize CLR context using class hierarchy.";
            LOG.log(Level.SEVERE, message, e);
            throw new RuntimeException(message, e);
        }
        jallocatedEvaluator.submitContextAndTask(contextConfiguration, taskConfiguration);
    }

    @Override
    public void close()
    {
        jallocatedEvaluator.close();
    }

    private static ClassHierarchy loadClassHierarchy(String classHierarchyFile) {
        try (final InputStream chin = new FileInputStream(classHierarchyFile)) {
            final ClassHierarchyProto.Node root = ClassHierarchyProto.Node.parseFrom(chin); // A
            final ClassHierarchy ch = new ProtocolBufferClassHierarchy(root);
            return ch;
        } catch (final IOException e) {
            final String message = "Unable to load class hierarchy from " + CLASS_HIERARCHY_FILENAME;
            LOG.log(Level.SEVERE, message, e);
            throw new RuntimeException(message, e);
        }
    }
}
