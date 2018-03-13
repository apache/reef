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
package org.apache.reef.runtime.azbatch.util.batch;

import com.microsoft.azure.batch.auth.BatchCredentials;
import com.microsoft.azure.batch.auth.BatchSharedKeyCredentials;
import org.apache.reef.runtime.azbatch.parameters.AzureBatchAccountKey;
import org.apache.reef.runtime.azbatch.parameters.AzureBatchAccountName;
import org.apache.reef.runtime.azbatch.parameters.AzureBatchAccountUri;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;

/**
 * An implementation of {@link IAzureBatchCredentialProvider} which returns {@link BatchSharedKeyCredentials}
 * for Azure Batch account.
 */
public class SharedKeyBatchCredentialProvider implements IAzureBatchCredentialProvider {

  private final String azureBatchAccountUri;
  private final String azureBatchAccountName;
  private final String azureBatchAccountKey;

  @Inject
  SharedKeyBatchCredentialProvider(@Parameter(AzureBatchAccountUri.class) final String azureBatchAccountUri,
                                   @Parameter(AzureBatchAccountName.class) final String azureBatchAccountName,
                                   @Parameter(AzureBatchAccountKey.class) final String azureBatchAccountKey) {
    this.azureBatchAccountUri = azureBatchAccountUri;
    this.azureBatchAccountName = azureBatchAccountName;
    this.azureBatchAccountKey = azureBatchAccountKey;
  }

  /**
   * Returns credentials for Azure Batch account.
   * @return an instance of {@link BatchSharedKeyCredentials} based on {@link AzureBatchAccountKey} parameter.
   */
  @Override
  public BatchCredentials getCredentials() {
    return new BatchSharedKeyCredentials(this.azureBatchAccountUri, this.azureBatchAccountName, azureBatchAccountKey);
  }
}
