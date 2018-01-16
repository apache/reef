package org.apache.reef.runime.azbatch.client;

import com.microsoft.azure.batch.BatchClient;
import com.microsoft.azure.batch.auth.BatchSharedKeyCredentials;
import com.microsoft.azure.batch.protocol.models.BatchErrorException;
import com.microsoft.azure.batch.protocol.models.JobAddParameter;
import com.microsoft.azure.batch.protocol.models.JobManagerTask;
import com.microsoft.azure.batch.protocol.models.PoolInformation;

import java.io.IOException;

public class AzureBatchJobSubmissionHelper implements AutoCloseable {

    private final String azureBatchAccountUri;
    private final String azureBatchAccountName;
    private final String azureBatchAccountKey;
    private final String azureBatchPoolId;

    private final String applicationId;

    public AzureBatchJobSubmissionHelper(
            String azureBatchAccountUri,
            String azureBatchAccountName,
            String azureBatchAccountKey,
            String azureBatchPoolId,
            String applicationId) {
        this.azureBatchAccountUri = azureBatchAccountUri;
        this.azureBatchAccountName = azureBatchAccountName;
        this.azureBatchAccountKey = azureBatchAccountKey;
        this.azureBatchPoolId = azureBatchPoolId;
        this.applicationId = applicationId;
    }

    public void submit() throws BatchErrorException, IOException  {

        BatchSharedKeyCredentials cred = new BatchSharedKeyCredentials(
                this.azureBatchAccountUri, this.azureBatchAccountName, this.azureBatchAccountKey);
        BatchClient client = BatchClient.open(cred);

        PoolInformation poolInfo = new PoolInformation();
        poolInfo.withPoolId(this.azureBatchPoolId);

        JobManagerTask jobManagerTask = new JobManagerTask()
                .withId(applicationId)
                .withCommandLine("dir");

        JobAddParameter jobAddParameter = new JobAddParameter()
                .withId(applicationId)
                .withJobManagerTask(jobManagerTask)
                .withPoolInfo(poolInfo);

        client.jobOperations().createJob(jobAddParameter);
    }

    @Override
    public void close() {

    }
}
