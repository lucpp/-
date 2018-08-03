/*
 * Copyright (C) 2011 The Guava Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package com.newbloom.common.hash;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.math.RoundingMode;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicLongArray;

import static java.lang.Math.abs;
import static java.math.RoundingMode.HALF_EVEN;
import static java.math.RoundingMode.HALF_UP;


/**
 * Collections of strategies of generating the k * log(M) bits required for an element to be mapped
 * to a BloomFilter of M bits and k hash functions. These strategies are part of the serialized form
 * of the Bloom filters that use them, thus they must be preserved as is (no updates allowed, only
 * introduction of new versions).
 *
 * <p>Important: the order of the constants cannot change, and they cannot be deleted - we depend on
 * their ordinal for BloomFilter serialization.
 *
 * @author Dimitris Andreou
 * @author Kurt Alfred Kluever
 */
public enum BloomFilterStrategies implements BloomFilter.Strategy {
    /**
     * See "Less Hashing, Same Performance: Building a Better Bloom Filter" by Adam Kirsch and Michael
     * Mitzenmacher. The paper argues that this trick doesn't significantly deteriorate the
     * performance of a Bloom filter (yet only needs two 32bit hash functions).
     */
    MURMUR128_MITZ_32() {
        @Override
        public <T> boolean put(
                T object, Funnel<? super T> funnel, int numHashFunctions, IBitArray bits) {
            long bitSize = bits.bitSize();
            long hash64 = Hashing.murmur3_128().hashObject(object, funnel).asLong();
            int hash1 = (int) hash64;
            int hash2 = (int) (hash64 >>> 32);

            boolean bitsChanged = false;
            for (int i = 1; i <= numHashFunctions; i++) {
                int combinedHash = hash1 + (i * hash2);
                // Flip all the bits if it's negative (guaranteed positive number)
                if (combinedHash < 0) {
                    combinedHash = ~combinedHash;
                }
                bitsChanged |= bits.set(combinedHash % bitSize);
            }
            return bitsChanged;
        }

        @Override
        public <T> boolean mightContain(
                T object, Funnel<? super T> funnel, int numHashFunctions, IBitArray bits) {
            long bitSize = bits.bitSize();
            long hash64 = Hashing.murmur3_128().hashObject(object, funnel).asLong();
            int hash1 = (int) hash64;
            int hash2 = (int) (hash64 >>> 32);

            for (int i = 1; i <= numHashFunctions; i++) {
                int combinedHash = hash1 + (i * hash2);
                // Flip all the bits if it's negative (guaranteed positive number)
                if (combinedHash < 0) {
                    combinedHash = ~combinedHash;
                }
                if (!bits.get(combinedHash % bitSize)) {
                    return false;
                }
            }
            return true;
        }
    },
    /**
     * This strategy uses all 128 bits of {@link Hashing#murmur3_128} when hashing. It looks different
     * than the implementation in MURMUR128_MITZ_32 because we're avoiding the multiplication in the
     * loop and doing a (much simpler) += hash2. We're also changing the index to a positive number by
     * AND'ing with Long.MAX_VALUE instead of flipping the bits.
     */
    MURMUR128_MITZ_64() {
        @Override
        public <T> boolean put(
                T object, Funnel<? super T> funnel, int numHashFunctions, IBitArray bits) {
            long bitSize = bits.bitSize();
            byte[] bytes = Hashing.murmur3_128().hashObject(object, funnel).getBytesInternal();
            long hash1 = lowerEight(bytes);
            long hash2 = upperEight(bytes);

            boolean bitsChanged = false;
            long combinedHash = hash1;
            for (int i = 0; i < numHashFunctions; i++) {
                // Make the combined hash positive and indexable
                bitsChanged |= bits.set((combinedHash & Long.MAX_VALUE) % bitSize);
                combinedHash += hash2;
            }
            return bitsChanged;
        }

        @Override
        public <T> boolean mightContain(
                T object, Funnel<? super T> funnel, int numHashFunctions, IBitArray bits) {
            long bitSize = bits.bitSize();
            byte[] bytes = Hashing.murmur3_128().hashObject(object, funnel).getBytesInternal();
            long hash1 = lowerEight(bytes);
            long hash2 = upperEight(bytes);

            long combinedHash = hash1;
            for (int i = 0; i < numHashFunctions; i++) {
                // Make the combined hash positive and indexable
                if (!bits.get((combinedHash & Long.MAX_VALUE) % bitSize)) {
                    return false;
                }
                combinedHash += hash2;
            }
            return true;
        }

        private long LongFromBytes(
                byte b1, byte b2, byte b3, byte b4, byte b5, byte b6, byte b7, byte b8) {
            return (b1 & 0xFFL) << 56
                    | (b2 & 0xFFL) << 48
                    | (b3 & 0xFFL) << 40
                    | (b4 & 0xFFL) << 32
                    | (b5 & 0xFFL) << 24
                    | (b6 & 0xFFL) << 16
                    | (b7 & 0xFFL) << 8
                    | (b8 & 0xFFL);
        }

        private /* static */ long lowerEight(byte[] bytes) {
            return LongFromBytes(
                    bytes[7], bytes[6], bytes[5], bytes[4], bytes[3], bytes[2], bytes[1], bytes[0]);
        }

        private /* static */ long upperEight(byte[] bytes) {
            return LongFromBytes(
                    bytes[15], bytes[14], bytes[13], bytes[12], bytes[11], bytes[10], bytes[9], bytes[8]);
        }
    };

    public static long LongMathDivide(long p, long q, RoundingMode mode) {

        long div = p / q; // throws if q == 0
        long rem = p - q * div; // equals p % q

        if (rem == 0) {
            return div;
        }

        /*
         * Normal Java division rounds towards 0, consistently with RoundingMode.DOWN. We just have to
         * deal with the cases where rounding towards 0 is wrong, which typically depends on the sign of
         * p / q.
         *
         * signum is 1 if p and q are both nonnegative or both negative, and -1 otherwise.
         */
        int signum = 1 | (int) ((p ^ q) >> (Long.SIZE - 1));
        boolean increment;
        switch (mode) {
            case UNNECESSARY:
                // checkRoundingUnnecessary(rem == 0);
                // fall through
            case DOWN:
                increment = false;
                break;
            case UP:
                increment = true;
                break;
            case CEILING:
                increment = signum > 0;
                break;
            case FLOOR:
                increment = signum < 0;
                break;
            case HALF_EVEN:
            case HALF_DOWN:
            case HALF_UP:
                long absRem = abs(rem);
                long cmpRemToHalfDivisor = absRem - (abs(q) - absRem);
                // subtracting two nonnegative longs can't overflow
                // cmpRemToHalfDivisor has the same sign as compare(abs(rem), abs(q) / 2).
                if (cmpRemToHalfDivisor == 0) { // exactly on the half mark
                    increment = (mode == HALF_UP | (mode == HALF_EVEN & (div & 1) != 0));
                } else {
                    increment = cmpRemToHalfDivisor > 0; // closer to the UP value
                }
                break;
            default:
                throw new AssertionError();
        }
        return increment ? div + signum : div;
    }

    interface IBitArray {
        boolean set(long bitIndex);

        boolean get(long bitIndex);

        long bitSize();

        long bitCount();
    }

    /**
     * Models a lock-free array of bits.
     *
     * <p>We use this instead of java.util.BitSet because we need access to the array of longs and we
     * need compare-and-swap.
     */
    static final class LockFreeBitArray implements IBitArray {
        private static final int LONG_ADDRESSABLE_BITS = 6;
        final AtomicLongArray data;
        private final LongAddable bitCount;

        LockFreeBitArray(long bits) {
            //this(new long[Ints.checkedCast(LongMathDivide(bits, 64, RoundingMode.CEILING))]);

            this(new long[(int) LongMathDivide(bits, 64, RoundingMode.CEILING)]);
        }

        // Used by serialization
        LockFreeBitArray(long[] data) {
//      checkArgument(data.length > 0, "data length is zero!");
            this.data = new AtomicLongArray(data);
            this.bitCount = LongAddables.create();
            long bitCount = 0;
            for (long value : data) {
                bitCount += Long.bitCount(value);
            }
            this.bitCount.add(bitCount);
        }

        /**
         * Returns true if the bit changed value.
         */
        public boolean set(long bitIndex) {
            if (get(bitIndex)) {
                return false;
            }

            int longIndex = (int) (bitIndex >>> LONG_ADDRESSABLE_BITS);
            long mask = 1L << bitIndex; // only cares about low 6 bits of bitIndex

            long oldValue;
            long newValue;
            do {
                oldValue = data.get(longIndex);
                newValue = oldValue | mask;
                if (oldValue == newValue) {
                    return false;
                }
            } while (!data.compareAndSet(longIndex, oldValue, newValue));

            // We turned the bit on, so increment bitCount.
            bitCount.increment();
            return true;
        }

        public boolean get(long bitIndex) {
            return (data.get((int) (bitIndex >>> 6)) & (1L << bitIndex)) != 0;
        }

        /**
         * Careful here: if threads are mutating the atomicLongArray while this method is executing, the
         * final long[] will be a "rolling snapshot" of the state of the bit array. This is usually good
         * enough, but should be kept in mind.
         */
        public static long[] toPlainArray(AtomicLongArray atomicLongArray) {
            long[] array = new long[atomicLongArray.length()];
            for (int i = 0; i < array.length; ++i) {
                array[i] = atomicLongArray.get(i);
            }
            return array;
        }

        /**
         * Number of bits
         */
        public long bitSize() {
            return (long) data.length() * Long.SIZE;
        }

        /**
         * Number of set bits (1s).
         *
         * <p>Note that because of concurrent set calls and uses of atomics, this bitCount is a (very)
         * close *estimate* of the actual number of bits set. It's not possible to do better than an
         * estimate without locking. Note that the number, if not exactly accurate, is *always*
         * underestimating, never overestimating.
         */
        public long bitCount() {
            return bitCount.sum();
        }

        LockFreeBitArray copy() {
            return new LockFreeBitArray(toPlainArray(data));
        }

        /**
         * Combines the two BitArrays using bitwise OR.
         *
         * <p>NOTE: Because of the use of atomics, if the other LockFreeBitArray is being mutated while
         * this operation is executing, not all of those new 1's may be set in the final state of this
         * LockFreeBitArray. The ONLY guarantee provided is that all the bits that were set in the other
         * LockFreeBitArray at the start of this method will be set in this LockFreeBitArray at the end
         * of this method.
         */
        public void putAll(LockFreeBitArray other) {
//      checkArgument(
//          data.length() == other.data.length(),
//          "BitArrays must be of equal length (%s != %s)",
//          data.length(),
//          other.data.length());
            for (int i = 0; i < data.length(); i++) {
                long otherLong = other.data.get(i);

                long ourLongOld;
                long ourLongNew;
                boolean changedAnyBits = true;
                do {
                    ourLongOld = data.get(i);
                    ourLongNew = ourLongOld | otherLong;
                    if (ourLongOld == ourLongNew) {
                        changedAnyBits = false;
                        break;
                    }
                } while (!data.compareAndSet(i, ourLongOld, ourLongNew));

                if (changedAnyBits) {
                    int bitsAdded = Long.bitCount(ourLongNew) - Long.bitCount(ourLongOld);
                    bitCount.add(bitsAdded);
                }
            }
        }

        @Override
        public boolean equals(Object o) {
            if (o instanceof LockFreeBitArray) {
                LockFreeBitArray lockFreeBitArray = (LockFreeBitArray) o;
                // TODO(lowasser): avoid allocation here
                return Arrays.equals(toPlainArray(data), toPlainArray(lockFreeBitArray.data));
            }
            return false;
        }

        @Override
        public int hashCode() {
            // TODO(lowasser): avoid allocation here
            return Arrays.hashCode(toPlainArray(data));
        }
    }

    public static final class FileBitArray implements IBitArray {
        private static final int LONG_ADDRESSABLE_BITS = 6;

        private final LongAddable bitCount;
        private RandomAccessFile raf = null;
        private long expectedLnegth = 0;
        private long bitArraySize = 0;
        private String filename = "";

        public FileBitArray(long bits, String filename) {
            this.filename = filename;
            expectedLnegth = (bits >>> 3) + 8;
            try {
                raf = new RandomAccessFile(filename, "rw");
                raf.setLength(expectedLnegth);
            } catch (IOException e) {
                e.printStackTrace();
            }
            bitArraySize=expectedLnegth*8;
            this.bitCount = LongAddables.create();

        }

        public static byte[] intToBytes(int value) {
            byte[] src = new byte[4];
            src[0] = (byte) ((value>>24) & 0xFF);
            src[1] = (byte) ((value>>16)& 0xFF);
            src[2] = (byte) ((value>>8)&0xFF);
            src[3] = (byte) (value & 0xFF);
            return src;
        }

        public static int bytesToInt(byte[] src, int offset) {
            int value;
            value = (int) ( ((src[offset] & 0xFF)<<24)
                    |((src[offset+1] & 0xFF)<<16)
                    |((src[offset+2] & 0xFF)<<8)
                    |(src[offset+3] & 0xFF));
            return value;
        }

        private int readFileIndex(long pos) {
            byte[] data = new byte[4];
            try {
                raf.seek(pos);
                raf.read(data, 0, 4);
            } catch (IOException e) {
                throw new IllegalArgumentException(e.toString());
            }

            return bytesToInt(data, 0);
        }

        private void writeFileIndex(long pos, int value) {
            try {
                raf.seek(pos);
                raf.write(intToBytes(value), 0, 4);

            } catch (IOException e) {
                throw new IllegalArgumentException(e.toString());
            }
        }

        public boolean set(long bitIndex) {
            if (get(bitIndex)) {
                return false;
            }

            long intIndex = (long) (bitIndex >>> 5);
            int mask = 1 << bitIndex;

            int oldValue;
            int newValue;

            oldValue = readFileIndex(intIndex * 4);
            newValue = (oldValue | mask);
            if (oldValue == newValue) {
                return false;
            }
            writeFileIndex(intIndex * 4, newValue);

            bitCount.increment();
            return true;
        }

        public boolean get(long bitIndex) {
            return (
                    readFileIndex((bitIndex >>> 5) * 4)
                            & (1 << bitIndex)
            ) != 0;
        }


        public long bitSize() {
            return bitArraySize;
        }


        public long bitCount() {
            return bitCount.sum();
        }

        FileBitArray copy() {
            throw new IllegalArgumentException("Does not support copy");
        }

        void putAll(FileBitArray other) {
            throw new IllegalArgumentException("Does not support putAll");
        }

        @Override
        public boolean equals(Object o) {
            return false;
        }

        @Override
        public int hashCode() {
            // TODO(lowasser): avoid allocation here
            return Arrays.hashCode(filename.getBytes());
        }
    }
}
