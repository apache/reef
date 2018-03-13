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

import com.microsoft.windowsazure.storage.StorageCredentialsSharedAccessSignature;
import com.microsoft.windowsazure.storage.StorageException;
import com.microsoft.windowsazure.storage.blob.CloudBlob;
import com.microsoft.windowsazure.storage.blob.CloudBlobClient;
import com.microsoft.windowsazure.storage.blob.SharedAccessBlobPolicy;
import com.microsoft.windowsazure.storage.core.PathUtility;
import com.microsoft.windowsazure.storage.core.UriQueryBuilder;
import org.apache.reef.runtime.azbatch.parameters.AzureStorageAccountName;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Cloud Blob client provider that uses Azure Storage Shared Access Signature authorization.
 */
public class SharedAccessSignatureCloudBlobClientProvider implements ICloudBlobClientProvider {

  private static final Logger LOG = Logger.getLogger(AzureStorageClient.class.getName());

  public static final String AZURE_STORAGE_CONTAINER_SAS_TOKEN_ENV = "AZURE_STORAGE_CONTAINER_SAS_TOKEN_ENV";

  private static final String AZURE_STORAGE_ACCOUNT_URI_FORMAT = "https://%s.blob.core.windows.net";

  private final String azureStorageAccountName;
  private final String azureStorageContainerSASToken;

  @Inject
  SharedAccessSignatureCloudBlobClientProvider(
      @Parameter(AzureStorageAccountName.class) final String azureStorageAccountName) {
    this.azureStorageAccountName = azureStorageAccountName;
    this.azureStorageContainerSASToken = System.getenv(AZURE_STORAGE_CONTAINER_SAS_TOKEN_ENV);
  }

  /**
   * Returns an instance of {@link CloudBlobClient} based on available authentication mechanism.
   * @return an instance of {@link CloudBlobClient}.
   * @throws IOException
   */
  @Override
  public CloudBlobClient getCloudBlobClient() throws IOException {
    StorageCredentialsSharedAccessSignature signature =
        new StorageCredentialsSharedAccessSignature(this.azureStorageContainerSASToken);
    URI storageAccountUri;
    try {
      storageAccountUri = new URI(String.format(AZURE_STORAGE_ACCOUNT_URI_FORMAT, this.azureStorageAccountName));
    } catch (URISyntaxException e) {
      throw new IOException("Failed to generate Storage Account URI", e);
    }

    return new CloudBlobClient(storageAccountUri, signature);
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
      final URI uri = cloudBlob.getStorageUri().getPrimaryUri();

      Map<String, String[]> queryString = PathUtility.parseQueryString(this.azureStorageContainerSASToken);

      UriQueryBuilder builder = new UriQueryBuilder();
      for (Map.Entry<String, String[]> entry : queryString.entrySet()) {
        for (String value : entry.getValue()) {
          builder.add(entry.getKey(), value);
        }
      }

      URI result = builder.addToURI(uri);
      LOG.log(Level.INFO, "Here's the URI: " + result);

      return result;
    } catch (StorageException | URISyntaxException e) {
      throw new IOException("Failed to generated a Shared Access Signature.", e);
    }
  }
}
