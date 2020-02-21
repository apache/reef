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

import com.microsoft.windowsazure.storage.blob.CloudBlob;
import com.microsoft.windowsazure.storage.blob.CloudBlobClient;
import com.microsoft.windowsazure.storage.blob.SharedAccessBlobPolicy;

import java.io.IOException;
import java.net.URI;

/**
 * An interface for classes that provide an instance of {@link CloudBlobClient} based
 * on available authentication mechanism.
 */
public interface ICloudBlobClientProvider {

  /**
   * Returns an instance of {@link CloudBlobClient} based on available authentication mechanism.
   * @return an instance of {@link CloudBlobClient}.
   * @throws IOException
   */
  CloudBlobClient getCloudBlobClient() throws IOException;

  /**
   * Generates a Shared Access Key URI for the given {@link CloudBlob}.
   * @param cloudBlob   cloud blob to create a Shared Access Key URI for.
   * @param policy      an instance of {@link SharedAccessBlobPolicy} that specifies permissions and signature's
   *                    validity time period.
   * @return            a Shared Access Key URI for the given {@link CloudBlob}.
   * @throws IOException
   */
  URI generateSharedAccessSignature(CloudBlob cloudBlob, SharedAccessBlobPolicy policy)
      throws IOException;
}
