/*
 * Copyright 2016 The Netty Project
 *
 * The Netty Project licenses this file to you under the Apache License, version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at:
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package net.qrono.server.redis;

import io.netty.util.CharsetUtil;
import io.netty.util.internal.PlatformDependent;

/**
 * Utilities for codec-redis.
 */
final class RedisCodecUtil {

  private RedisCodecUtil() {
  }

  static int longStrLen(long value) {
    int signDigits;
    if (value < 0) {
      signDigits = 1;
    } else {
      signDigits = 0;
      value = -value;
    }

    // Long.MAX_VALUE = 9 223 372 036 854 775 807
    // 19 digits

    long pow = -10;
    for (int i = 1; i <= 19; i++) {
      if (value > pow) {
        return i + signDigits;
      }
      pow *= 10;
    }

    return 19 + signDigits;
  }

  static byte[] longToAsciiBytes(long value) {
    return Long.toString(value).getBytes(CharsetUtil.US_ASCII);
  }

  /**
   * Returns a {@code short} value using endian order.
   */
  static short makeShort(char first, char second) {
    return PlatformDependent.BIG_ENDIAN_NATIVE_ORDER ?
        (short) ((second << 8) | first) : (short) ((first << 8) | second);
  }
}
