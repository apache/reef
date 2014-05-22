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
package com.microsoft.reef.io.network.util;

import com.google.protobuf.ByteString;
import com.microsoft.reef.io.network.proto.ReefNetworkGroupCommProtos.GroupCommMessage;
import com.microsoft.reef.io.network.proto.ReefNetworkGroupCommProtos.GroupCommMessage.Type;
import com.microsoft.reef.io.network.proto.ReefNetworkGroupCommProtos.GroupMessageBody;
import com.microsoft.wake.ComparableIdentifier;
import com.microsoft.wake.Identifier;
import com.microsoft.wake.IdentifierFactory;

import java.net.Inet4Address;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class Utils {

  private static final String DELIMITER = "-";

  public static List<Identifier> parseList(String ids, IdentifierFactory factory) {
    List<Identifier> result = new ArrayList<>();
    String[] tokens = ids.split(DELIMITER);
    for (String token : tokens) {
      result.add(factory.getNewInstance(token.trim()));
    }
    return result;
  }

  public static List<ComparableIdentifier> parseListCmp(String ids, IdentifierFactory factory) {
    List<ComparableIdentifier> result = new ArrayList<>();
    String[] tokens = ids.split(DELIMITER);
    for (String token : tokens) {
      result.add((ComparableIdentifier) factory.getNewInstance(token.trim()));
    }
    return result;
  }

  public static String listToString(List<ComparableIdentifier> ids) {
    if (ids == null || ids.size() == 0) return "";
    StringBuilder sb = new StringBuilder();
    for (ComparableIdentifier comparableIdentifier : ids) {
      sb.append(comparableIdentifier.toString());
      sb.append(DELIMITER);
    }
    sb.deleteCharAt(sb.length() - 1);
    return sb.toString();
  }

  public static List<Integer> createUniformCounts(int elemSize, int childSize) {
    List<Integer> result = new ArrayList<>(childSize);
    int remainder = elemSize % childSize;
    int quotient = elemSize / childSize;
    for (int i = 0; i < remainder; i++)
      result.add(quotient + 1);
    for (int i = remainder; i < childSize; i++)
      result.add(quotient);
    return result;
  }

  public static class Pair<T1, T2> {
    public final T1 first;
    public final T2 second;

    public Pair(T1 first, T2 second) {
      this.first = first;
      this.second = second;
    }

    @Override
    public String toString() {
      return "(" + first + "," + second + ")";
    }
  }

  private static class AddressComparator implements Comparator<Inet4Address> {

    @Override
    public int compare(final Inet4Address aa, final Inet4Address ba) {
      final byte[] a = aa.getAddress();
      final byte[] b = ba.getAddress();
      // local subnet comes after all else.
      if (a[0] == 127 && b[0] != 127) {
        return 1;
      }
      if (a[0] != 127 && b[0] == 127) {
        return -1;
      }
      for (int i = 0; i < 4; i++) {
        if (a[i] < b[i]) {
          return -1;
        }
        if (a[i] > b[i]) {
          return 1;
        }
      }
      return 0;
    }
  }

  public static GroupCommMessage bldGCM(
      final Type msgType, final Identifier from, final Identifier to, final byte[]... elements) {

    final GroupCommMessage.Builder GCMBuilder = GroupCommMessage.newBuilder();
    GCMBuilder.setType(msgType);
    GCMBuilder.setSrcid(from.toString());
    GCMBuilder.setDestid(to.toString());

    final GroupMessageBody.Builder bodyBuilder = GroupMessageBody.newBuilder();
    for (final byte[] element : elements) {
      bodyBuilder.setData(ByteString.copyFrom(element));
      GCMBuilder.addMsgs(bodyBuilder.build());
    }

    return GCMBuilder.build();
  }
}