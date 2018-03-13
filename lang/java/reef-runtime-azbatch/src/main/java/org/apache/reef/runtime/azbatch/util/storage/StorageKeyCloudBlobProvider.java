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
package org.apache.reef.runtime.azbatch.util.storage;

import com.microsoft.windowsazure.storage.CloudStorageAccount;
import com.microsoft.windowsazure.storage.StorageException;
import com.microsoft.windowsazure.storage.blob.CloudBlob;
import com.microsoft.windowsazure.storage.blob.CloudBlobClient;
import com.microsoft.windowsazure.storage.blob.SharedAccessBlobPolicy;
import org.apache.reef.runtime.azbatch.parameters.AzureStorageAccountKey;
import org.apache.reef.runtime.azbatch.parameters.AzureStorageAccountName;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.InvalidKeyException;

/**
 * Cloud Blob client provider that uses Azure Storage Shared Key authorization.
 */
public class StorageKeyCloudBlobProvider implements ICloudBlobClientProvider {

  private static final String AZURE_STORAGE_CONNECTION_STRING_FORMAT =
      "DefaultEndpointsProtocol=https;AccountName=%s;AccountKey=%s";

  private final String azureStorageAccountName;
  private final String azureStorageAccountKey;

  @Inject
  StorageKeyCloudBlobProvider(
      @Parameter(AzureStorageAccountName.class) final String azureStorageAccountName,
      @Parameter(AzureStorageAccountKey.class) final String azureStorageAccountKey) {
    this.azureStorageAccountName = azureStorageAccountName;
    this.azureStorageAccountKey = azureStorageAccountKey;
  }

  /**
   * Returns an instance of {@link CloudBlobClient} based on available authentication mechanism.
   * @return an instance of {@link CloudBlobClient}.
   * @throws IOException
   */
  @Override
  public CloudBlobClient getCloudBlobClient() throws IOException {
    String connectionString =  String.format(AZURE_STORAGE_CONNECTION_STRING_FORMAT,
        this.azureStorageAccountName, this.azureStorageAccountKey);
    try {
      return CloudStorageAccount.parse(connectionString).createCloudBlobClient();
    } catch (URISyntaxException | InvalidKeyException e) {
      throw new IOException("Failed to create a Cloud Storage Account.", e);
    }
  }

  /**
   * Generates a Shared Access Key URI for the given {@link CloudBlob}.
   * @param cloudBlob   cloud blob to create a Shared Access Key URI for.
   * @param policy      an instance of {@link SharedAccessBlobPolicy} that specifies permissions and signature's
   *                    validity time period.
   * @return            a Shared Access Key URI for the given {@link CloudBlob}.
   * @throws IOException
   */
  @Override
  public URI generateSharedAccessSignature(final CloudBlob cloudBlob, final SharedAccessBlobPolicy policy)
      throws IOException {
    try {
      final String sas = cloudBlob.generateSharedAccessSignature(policy, null);
      final String uri = cloudBlob.getStorageUri().getPrimaryUri().toString();
      return new URI(uri + "?" + sas);
    } catch (StorageException | InvalidKeyException | URISyntaxException e) {
      throw new IOException("Failed to generated a Shared Access Signature.", e);
    }
  }
}
