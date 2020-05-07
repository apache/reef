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
package org.apache.reef.bridge.client;

import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.DatumReader;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.reef.runtime.common.files.REEFFileNames;

import javax.inject.Inject;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Helper class to read security tokens from file and add them to the user's credentials.
 * .Net SecurityTokenWriter stores the token info in a file and passes it to the java client.
 * The method in this class will read this data and add tokens to UserGroupInformation.
 */
final class SecurityTokensReader {

  private static final Logger LOG = Logger.getLogger(SecurityTokensReader.class.getName());

  private final DatumReader<SecurityToken> tokenDatumReader = new SpecificDatumReader<>(SecurityToken.class);
  private final DecoderFactory decoderFactory = new DecoderFactory();
  private final File securityTokensFile;

  @Inject
  private SecurityTokensReader(final REEFFileNames reefFileNames) {
    this.securityTokensFile = new File(reefFileNames.getSecurityTokensFile());
  }

  /**
   * Read tokens from a file and add them to the user's credentials.
   * @param ugi user's credentials to add tokens to.
   * @throws IOException if there are errors in reading the tokens' file.
   */
  void addTokensFromFile(final UserGroupInformation ugi) throws IOException {
    LOG.log(Level.FINE, "Reading security tokens from file: {0}", this.securityTokensFile);

    try (FileInputStream stream = new FileInputStream(securityTokensFile)) {
      final BinaryDecoder decoder = decoderFactory.binaryDecoder(stream, null);

      while (!decoder.isEnd()) {
        final SecurityToken token = tokenDatumReader.read(null, decoder);

        final Token<TokenIdentifier> yarnToken = new Token<>(
            token.getKey().array(),
            token.getPassword().array(),
            new Text(token.getKind().toString()),
            new Text(token.getService().toString()));

        LOG.log(Level.FINE, "addToken for {0}", yarnToken.getKind());

        ugi.addToken(yarnToken);
      }
    }
  }
}
