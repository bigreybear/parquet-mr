/**
 * Copyright 2012 Twitter, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package parquet.column.values.bitpacking;

import static parquet.Log.DEBUG;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import parquet.Log;
import parquet.bytes.BytesInput;

/**
 * Uses the generated Byte based bit packing to write ints into a BytesInput
 *
 * @author Julien Le Dem
 *
 */
public class ByteBasedBitPackingEncoder {
  private static final Log LOG = Log.getLog(ByteBasedBitPackingEncoder.class);

  private static final int VALUES_WRITTEN_AT_A_TIME = 8;
  /** must be a multiple of VALUES_WRITTEN_AT_A_TIME */
  private static final int SLAB_SIZE = VALUES_WRITTEN_AT_A_TIME * 1024 * 1024;

  private final int bitWidth;
  private final BytePacker packer;
  private final int[] input = new int[VALUES_WRITTEN_AT_A_TIME];
  private int inputSize;
  private byte[] packed;
  private int packedPosition;
  private final List<BytesInput> slabs = new ArrayList<BytesInput>();
  private int totalValues;

  /**
   * @param bitWidth the number of bits used to encode an int
   */
  public ByteBasedBitPackingEncoder(int bitWidth) {
    this.bitWidth = bitWidth;
    this.inputSize = 0;
    initPackedSlab();
    packer = ByteBitPacking.getPacker(bitWidth);
  }

  /**
   * writes an int using the requested number of bits.
   * accepts only value < 2^bitWidth
   * @param value the value to write
   * @throws IOException
   */
  public void writeInt(int value) throws IOException {
    input[inputSize] = value;
    ++ inputSize;
    if (inputSize == VALUES_WRITTEN_AT_A_TIME) {
      pack();
      if (packedPosition == SLAB_SIZE) {
        slabs.add(BytesInput.from(packed));
        initPackedSlab();
      }
    }
  }

  private void pack() {
    packer.pack8Values(input, 0, packed, packedPosition);
    packedPosition += bitWidth;
    totalValues += inputSize;
    inputSize = 0;
  }

  private void initPackedSlab() {
    packed = new byte[SLAB_SIZE];
    packedPosition = 0;
  }

  /**
   * @return the bytes representing the packed values
   * @throws IOException
   */
  public BytesInput toBytes() throws IOException {
    int packedByteLength = packedPosition + (inputSize * bitWidth + 7) / 8;
    if (DEBUG) LOG.debug("writing " + (slabs.size() * SLAB_SIZE + packedByteLength) + " bytes");
    if (inputSize > 0) {
      for (int i = inputSize; i < input.length; i++) {
        input[i] = 0;
      }
      pack();
    }
    return BytesInput.fromSequence(
        BytesInput.fromSequence(slabs),
        BytesInput.from(packed, 0, packedByteLength)
        );
  }

  /**
   * @return size of the data as it would be written
   */
  public long getBufferSize() {
    return (totalValues * bitWidth + 7) / 8;
  }

  /**
   * @return total memory allocated
   */
  public long getAllocatedSize() {
    return (slabs.size() * SLAB_SIZE) + packed.length + input.length * 4;
  }

}
