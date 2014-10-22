/**
 * Copyright (C) 2014 Microsoft Corporation
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
package com.microsoft.reef.io.network.naming.serialization;

import com.microsoft.wake.remote.Codec;

/**
 * naming registration response codec
 */
public class NamingRegisterResponseCodec implements Codec<NamingRegisterResponse>{
  private final NamingRegisterRequestCodec codec;
  
  /**
   * Constructs a naming register response codec
   * 
   * @param codec the naming register request codec
   */
  public NamingRegisterResponseCodec(NamingRegisterRequestCodec codec) {
    this.codec = codec;
  }

  /**
   * Encodes a naming register response to bytes
   * 
   * @param obj the naming register response
   * @return bytes a byte array
   */
  @Override
  public byte[] encode(NamingRegisterResponse obj) {
    return codec.encode(obj.getRequest());
  }

  /**
   * Decodes a naming register response from the bytes
   * 
   * @param buf the byte array
   * @return a naming register response
   */
  @Override
  public NamingRegisterResponse decode(byte[] buf) {
    return new NamingRegisterResponse(codec.decode(buf));
  }
  
}
