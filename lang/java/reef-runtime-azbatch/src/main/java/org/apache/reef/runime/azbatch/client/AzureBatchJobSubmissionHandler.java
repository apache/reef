/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.reef.runime.azbatch.client;

import org.apache.reef.annotations.audience.Private;
import org.apache.reef.runime.azbatch.parameters.AzureBatchAccountKey;
import org.apache.reef.runime.azbatch.parameters.AzureBatchAccountName;
import org.apache.reef.runime.azbatch.parameters.AzureBatchAccountUri;
import org.apache.reef.runime.azbatch.parameters.AzureBatchPoolId;
import org.apache.reef.runtime.common.client.api.JobSubmissionEvent;
import org.apache.reef.runtime.common.client.api.JobSubmissionHandler;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.io.IOException;
import java.util.Date;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A {@link JobSubmissionHandler} for Azure Batch.
 */
@Private
public final class AzureBatchJobSubmissionHandler implements JobSubmissionHandler {

    private static final Logger LOG = Logger.getLogger(AzureBatchJobSubmissionHandler.class.getName());

    private final String applicationId;

    private final String azureBatchAccountUri;
    private final String azureBatchAccountName;
    private final String azureBatchAccountKey;
    private final String azureBatchPoolId;

    @Inject
    AzureBatchJobSubmissionHandler(
            @Parameter(AzureBatchAccountUri.class) final String azureBatchAccountUri,
            @Parameter(AzureBatchAccountName.class) final String azureBatchAccountName,
            @Parameter(AzureBatchAccountKey.class) final String azureBatchAccountKey,
            @Parameter(AzureBatchPoolId.class) final String azureBatchPoolId) {
        this.azureBatchAccountUri = azureBatchAccountUri;
        this.azureBatchAccountName = azureBatchAccountName;
        this.azureBatchAccountKey = azureBatchAccountKey;
        this.azureBatchPoolId = azureBatchPoolId;

        this.applicationId = "HelloWorldJob-"
                + this.azureBatchAccountName + "-"
                + (new Date()).toString()
                .replace(' ', '-')
                .replace(':', '-')
                .replace('.', '-');
    }

    @Override
    public String getApplicationId() {
        return this.applicationId;
    }

    @Override
    public void close() throws Exception {
        LOG.log(Level.INFO, "Closing " + AzureBatchJobSubmissionHandler.class.getName());
    }

    @Override
    public void onNext(final JobSubmissionEvent jobSubmissionEvent) {
        final String id = jobSubmissionEvent.getIdentifier();
        LOG.log(Level.FINEST, "Submitting job: {0}", jobSubmissionEvent);

        try (final AzureBatchJobSubmissionHelper helper = new AzureBatchJobSubmissionHelper(
                this.azureBatchAccountUri,
                this.azureBatchAccountName,
                this.azureBatchAccountKey,
                this.azureBatchPoolId,
                getApplicationId())) {
            helper.submit();
        }
        catch (IOException e) {
            LOG.log(Level.WARNING, "Failed to create the Driver application.");
        }
    }
}
