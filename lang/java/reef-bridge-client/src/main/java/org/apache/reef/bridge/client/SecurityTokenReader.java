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

import org.apache.reef.runtime.common.files.REEFFileNames;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Helper class to provide security token methods.
 * .Net SecurityTokenWriter stores the token info into files and passes it to the java client.
 * The methods in this class will read data from corresponding files and create TokenInfo objects.
 * It also provides method to add token to UserGroupInformation.
 */
final class SecurityTokenReader {

  private static final Logger LOG = Logger.getLogger(SecurityTokenReader.class.getName());
  private static final String TOKEN_PART_SEPARATOR = ",";
  private static final String TOKEN_KIND_SEPARATOR = ":";

  private SecurityTokenReader() {
  }

  /**
   * Read token info from token files and YarnClusterSubmissionFromCS.
   * @param yarnSubmission - YarnClusterSubmissionFromCS object that contains token kind and token service info.
   * @return List<TokenInfo> - returns list of TokenInfo object.
   * @throws IOException will be thrown if there are errors in reading token files.
   */
  static List<TokenInfo> readTokens(final YarnClusterSubmissionFromCS yarnSubmission) throws IOException {
    final REEFFileNames fileNames = new REEFFileNames();
    final String securityTokenIdentifierFile = fileNames.getSecurityTokenIdentifierFile();
    final String securityTokenPasswordFile = fileNames.getSecurityTokenPasswordFile();

    final String tokenKinds = yarnSubmission.getTokenKind();
    LOG.log(Level.INFO, "Entire tokenKinds from config: {0}.", tokenKinds);
    final String[] tokenKindString = tokenKinds.split(SecurityTokenReader.TOKEN_KIND_SEPARATOR);

    if (tokenKindString.length == 1) {  // to support backward compatibility for one token
      final TokenInfo token = readToken(securityTokenIdentifierFile, securityTokenPasswordFile, yarnSubmission);
      final List<TokenInfo> tokens = new ArrayList<>();
      tokens.add(token);
      return tokens;
    } else { //For multiple tokens
      return readTokens(securityTokenIdentifierFile, securityTokenPasswordFile, tokenKindString);
    }
  }

  /**
   * Read single token data.
   * @param securityTokenIdentifierFile - the file that contains one or more security token ids.
   * @param securityTokenPasswordFile - the file that contains one or more security token passwords.
   * @param yarnSubmission - YarnClusterSubmissionFromCS object that contains job submission parameters.
   * @return TokenInfo - returns TokenInfo object
   * @throws IOException will be thrown if there are errors in reading token files.
   */
  private static TokenInfo readToken(final String securityTokenIdentifierFile,
                             final String securityTokenPasswordFile,
                             final YarnClusterSubmissionFromCS yarnSubmission) throws IOException {
    final byte[] identifier = Files.readAllBytes(Paths.get(securityTokenIdentifierFile));
    final byte[] password = Files.readAllBytes(Paths.get(securityTokenPasswordFile));
    final TokenInfo tokenInfo = new TokenInfo(yarnSubmission.getTokenKind(),
        yarnSubmission.getTokenService(), identifier.length);
    tokenInfo.setKey(identifier);
    tokenInfo.setPassword(password);
    return tokenInfo;
  }
  /**
   * Reads token info from files and returns a list of TokenInfo.
   *
   * @param securityTokenIdentifierFile - the file that contains one or more security token ids.
   * @param securityTokenPasswordFile - the file that contains one or more security token passwords.
   * @param tokenKindString - array of strings that contains token kind strings.
   * @return return a list of TokenInfo objects.
   * @throws IOException will be thrown if there are errors in reading token files.
   */
  private static List<TokenInfo> readTokens(final String securityTokenIdentifierFile,
                                      final String securityTokenPasswordFile,
                                      final String[] tokenKindString) throws IOException {

    final List<TokenInfo> tokens = new ArrayList<>();
    for (final String tokenStr : tokenKindString) {
      LOG.log(Level.FINE, "tokenKind for each token:{0}.", tokenStr);
      final String[] tokenParts = tokenStr.split(TOKEN_PART_SEPARATOR);
      if (tokenParts.length != 2) {
        LOG.log(Level.SEVERE, "Expected token parts length is 2 with format [tokenKind, tokenLen].");
        throw new IllegalArgumentException("Expected token parts length is 2");
      }

      tokens.add(new TokenInfo(tokenParts[0], tokenParts[0], Integer.parseInt(tokenParts[1])));
    }

    try (final BufferedReader reader = new BufferedReader(
            new InputStreamReader(new FileInputStream(securityTokenPasswordFile), "UTF-8"))) {
      for (final TokenInfo info : tokens) {
        String line = reader.readLine();
        if (line != null) {
          info.setPassword(line.getBytes("UTF-8"));
        } else {
          throw new IllegalArgumentException("Password file doesn't contain expected password");
        }
      }
    }

    try (final DataInputStream din = new DataInputStream(new FileInputStream(securityTokenIdentifierFile))) {
      for (final TokenInfo info : tokens) {
        final int len = info.getLength();
        final byte[] b = new byte[len];
        final int lenRead = din.read(b, 0, len);
        LOG.log(Level.FINE, "Token length read: {0}.", lenRead);
        info.setKey(b);
      }
    }
    return tokens;
  }
}
