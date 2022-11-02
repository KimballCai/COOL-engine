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

package com.nus.cool.core.io.compression;

import com.nus.cool.core.util.IntegerUtil;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * Compress data sequences in which same data value occurs in many consecutive data elements are
 * stored as single data value and count.
 *
 * <p>Data layout --------------------------------------- | zlen | segments | compressed values |
 * --------------------------------------- where segments = number of segment in values
 *
 * <p>compressed values layout ------------------------ | value | offset | len |
 * ------------------------
 */
public class RLECompressor implements Compressor {

  /** Bytes number of z len and number of segments. */
  public static final int HEADACC = 4 + 4;

  private final int maxCompressedLen;

  public RLECompressor(Histogram hist) {
    int uncompressedSize = 3 * Integer.BYTES * hist.getNumOfValues();
    this.maxCompressedLen = HEADACC + (Math.max(hist.getRawSize(), uncompressedSize));
  }

  /**
   * Write int value to a given buffer.
   *
   * @param buf buffer to be filled
   * @param v value
   * @param width length of the valuen
   */
  private void writeInt(ByteBuffer buf, int v, int width) {
    switch (width) {
      case 1:
        buf.put((byte) v);
        break;
      case 2:
        buf.putShort((short) v);
        break;
      case 3:
      case 0:
        buf.putInt(v);
        break;
      default:
        throw new java.lang.IllegalArgumentException("incorrect number of bytes");
    }
  }

  /**
   * Add value to a given buffer.
   *
   * @param buf the buffet to filled
   * @param val value
   * @param off offset of the buffer
   * @param len how many the value has been repeated
   */
  private void write(ByteBuffer buf, int val, int off, int len) {
    byte b = 0; // store value's width + offset's width + len's width
    b |=
        ((IntegerUtil.minBytes(val) << 4)
            | IntegerUtil.minBytes(off) << 2
            | IntegerUtil.minBytes(len));
    buf.put(b);

    writeInt(buf, val, ((b >> 4) & 3)); // get upper 2 bites
    writeInt(buf, off, ((b >> 2) & 3)); // get middle 2 bites
    writeInt(buf, len, ((b) & 3)); // get lower 2 bites
  }

  @Override
  public int maxCompressedLength() {
    return this.maxCompressedLen;
  }

  @Override
  public int compress(
      byte[] src, int srcOff, int srcLen, byte[] dest, int destOff, int maxDestLen) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int compress(int[] src, int srcOff, int srcLen, byte[] dest, int destOff, int maxDestLen) {
    int n = 1; // how many distinct value
    ByteBuffer buf = ByteBuffer.wrap(dest, destOff, maxDestLen).order(ByteOrder.nativeOrder());
    buf.position(HEADACC);
    int v = src[srcOff];
    int voff = 0;
    int vlen = 1;
    // for each record,
    for (int i = srcOff + 1; i < srcOff + srcLen; i++) {
      if (src[i] != v) {
        write(buf, v, voff, vlen);
        v = src[i];
        // re-init offset in output buffer, and length of distinct value
        voff = i - srcOff;
        vlen = 1;
        n++;
      } else {
        vlen++;
      }
    }
    write(buf, v, voff, vlen);
    int zlen = buf.position() - HEADACC;
    buf.position(0);
    buf.putInt(zlen).putInt(n);
    return zlen + HEADACC;
  }
}
