/**
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
package org.apache.reef.runtime.hdinsight.client.yarnrest;

import org.codehaus.jackson.annotate.JsonProperty;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by afchung on 4/4/15.
 */
public class Credentials {

    private Map<String, List<StringEntry>> tokens = new HashMap<>();
    private Map<String, List<StringEntry>> secrets = new HashMap<>();

    public Credentials() {
        this.tokens.put(Constants.ENTRY, new ArrayList<StringEntry>());
        this.secrets.put(Constants.ENTRY, new ArrayList<StringEntry>());
    }

    public Credentials addSecret(String key, String value) {
        if (!this.secrets.containsKey(Constants.ENTRY)) {
            this.secrets.put(Constants.ENTRY, new ArrayList<StringEntry>());
        }
        this.secrets.get(Constants.ENTRY).add(new StringEntry(key, value));
        return this;
    }

    public Credentials addToken(String key, String value) {
        if (!this.tokens.containsKey(Constants.ENTRY)) {
            this.tokens.put(Constants.ENTRY, new ArrayList<StringEntry>());
        }
        this.tokens.get(Constants.ENTRY).add(new StringEntry(key, value));
        return this;
    }

    @JsonProperty(Constants.SECRETS)
    public Map<String, List<StringEntry>> getSecrets() {
        return this.secrets;
    }

    public Credentials setSecrets(final Map<String, List<StringEntry>> secrets) {
        this.secrets = secrets;
        return this;
    }

    @JsonProperty(Constants.TOKENS)
    public Map<String, List<StringEntry>> getTokens() {
        return this.tokens;
    }

    public Credentials setTokens(final Map<String, List<StringEntry>> tokens) {
        this.tokens = tokens;
        return this;
    }
}
