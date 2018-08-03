
package com.newbloom.common.hash;

import com.newbloom.common.hash.BloomFilterStrategies.LockFreeBitArray;

import java.io.Serializable;
import java.util.Arrays;
import java.util.stream.Collector;

import static java.lang.Math.abs;
import static java.lang.Math.copySign;
import static java.lang.Math.rint;


public final class BloomFilter<T> implements Serializable {
    /**
     * A strategy to translate T instances, to {@code numHashFunctions} bit indexes.
     *
     * <p>Implementations should be collections of pure functions (i.e. stateless).
     */
    interface Strategy extends java.io.Serializable {

        /**
         * Sets {@code numHashFunctions} bits of the given bit array, by hashing a user element.
         *
         * <p>Returns whether any bits changed as a result of this operation.
         */
        <T> boolean put(
                T object, Funnel<? super T> funnel, int numHashFunctions, BloomFilterStrategies.IBitArray bits);

        /**
         * Queries {@code numHashFunctions} bits of the given bit array, by hashing a user element;
         * returns {@code true} if and only if all selected bits are set.
         */
        <T> boolean mightContain(
                T object, Funnel<? super T> funnel, int numHashFunctions, BloomFilterStrategies.IBitArray bits);

        /**
         * Identifier used to encode this strategy, when marshalled as part of a BloomFilter. Only
         * values in the [-128, 127] range are valid for the compact serial form. Non-negative values
         * are reserved for enums defined in BloomFilterStrategies; negative values are reserved for any
         * custom, stateful strategy we may define (e.g. any kind of strategy that would depend on user
         * input).
         */
        int ordinal();
    }

    /**
     * The bit set of the BloomFilter (not necessarily power of 2!)
     */
    private final BloomFilterStrategies.IBitArray bits;

    /**
     * Number of hashes per element
     */
    private final int numHashFunctions;

    /**
     * The funnel to translate Ts to bytes
     */
    private final Funnel<? super T> funnel;

    /**
     * The strategy we employ to map an element T to {@code numHashFunctions} bit indexes.
     */
    private final Strategy strategy;

    /**
     * Creates a BloomFilter.
     */
    private BloomFilter(
            BloomFilterStrategies.IBitArray bits, int numHashFunctions, Funnel<? super T> funnel, Strategy strategy) {
//    checkArgument(numHashFunctions > 0, "numHashFunctions (%s) must be > 0", numHashFunctions);
//    checkArgument(
//        numHashFunctions <= 255, "numHashFunctions (%s) must be <= 255", numHashFunctions);
        this.bits = bits;
        this.numHashFunctions = numHashFunctions;
        this.funnel = funnel;
        this.strategy = strategy;
    }

    /**
     * Creates a new {@code BloomFilter} that's a copy of this instance. The new instance is equal to
     * this instance but shares no mutable state.
     *
     * @since 12.0
     */
//  public BloomFilter<T> copy() {
//    return new BloomFilter<T>(bits.copy(), numHashFunctions, funnel, strategy);
//  }

    /**
     * Returns {@code true} if the element <i>might</i> have been put in this Bloom filter, {@code
     * false} if this is <i>definitely</i> not the case.
     */
    public boolean mightContain(T object) {
        return strategy.mightContain(object, funnel, numHashFunctions, bits);
    }

//  /**
//   * @deprecated Provided only to satisfy the {@link Predicate} interface; use {@link #mightContain}
//   *     instead.
//   */
//  @Deprecated
//  @Override
//  public boolean apply(T input) {
//    return mightContain(input);
//  }

    /**
     * Puts an element into this {@code BloomFilter}. Ensures that subsequent invocations of {@link
     * #mightContain(Object)} with the same element will always return {@code true}.
     *
     * @return true if the Bloom filter's bits changed as a result of this operation. If the bits
     * changed, this is <i>definitely</i> the first time {@code object} has been added to the
     * filter. If the bits haven't changed, this <i>might</i> be the first time {@code object} has
     * been added to the filter. Note that {@code put(t)} always returns the <i>opposite</i>
     * result to what {@code mightContain(t)} would have returned at the time it is called.
     * @since 12.0 (present in 11.0 with {@code void} return type})
     */
    public boolean put(T object) {
        return strategy.put(object, funnel, numHashFunctions, bits);
    }

    /**
     * Returns the probability that {@linkplain #mightContain(Object)} will erroneously return {@code
     * true} for an object that has not actually been put in the {@code BloomFilter}.
     *
     * <p>Ideally, this number should be close to the {@code fpp} parameter passed in {@linkplain
     * #create(Funnel, int, double)}, or smaller. If it is significantly higher, it is usually the
     * case that too many elements (more than expected) have been put in the {@code BloomFilter},
     * degenerating it.
     *
     * @since 14.0 (since 11.0 as expectedFalsePositiveProbability())
     */
    public double expectedFpp() {
        // You down with FPP? (Yeah you know me!) Who's down with FPP? (Every last homie!)
        return Math.pow((double) bits.bitCount() / bitSize(), numHashFunctions);
    }


    /**
     * Returns an estimate for the total number of distinct elements that have been added to this
     * Bloom filter. This approximation is reasonably accurate if it does not exceed the value of
     * {@code expectedInsertions} that was used when constructing the filter.
     *
     * @since 22.0
     */
    public long approximateElementCount() {
        long bitSize = bits.bitSize();
        long bitCount = bits.bitCount();

        /**
         * Each insertion is expected to reduce the # of clear bits by a factor of
         * `numHashFunctions/bitSize`. So, after n insertions, expected bitCount is `bitSize * (1 - (1 -
         * numHashFunctions/bitSize)^n)`. Solving that for n, and approximating `ln x` as `x - 1` when x
         * is close to 1 (why?), gives the following formula.
         */
        double fractionOfBitsSet = (double) bitCount / bitSize;
//    return DoubleMath.roundToLong(
//        -Math.log1p(-fractionOfBitsSet) * bitSize / numHashFunctions, RoundingMode.HALF_UP);

        double x = -Math.log1p(-fractionOfBitsSet) * bitSize / numHashFunctions;
        double z = rint(x);
        if (abs(x - z) == 0.5) {
            return (long) (x + copySign(0.5, x));
        } else {
            return (long) z;
        }
    }


    long bitSize() {
        return bits.bitSize();
    }

    /**
     * Determines whether a given Bloom filter is compatible with this Bloom filter. For two Bloom
     * filters to be compatible, they must:
     *
     * <ul>
     * <li>not be the same instance
     * <li>have the same number of hash functions
     * <li>have the same bit size
     * <li>have the same strategy
     * <li>have equal funnels
     * </ul>
     *
     * @param that The Bloom filter to check for compatibility.
     * @since 15.0
     */
    public boolean isCompatible(BloomFilter<T> that) {
        // checkNotNull(that);
        return this != that
                && this.numHashFunctions == that.numHashFunctions
                && this.bitSize() == that.bitSize()
                && this.strategy.equals(that.strategy)
                && this.funnel.equals(that.funnel);
    }

    @Override
    public boolean equals(Object object) {
        if (object == this) {
            return true;
        }
        if (object instanceof BloomFilter) {
            BloomFilter<?> that = (BloomFilter<?>) object;
            return this.numHashFunctions == that.numHashFunctions
                    && this.funnel.equals(that.funnel)
                    && this.bits.equals(that.bits)
                    && this.strategy.equals(that.strategy);
        }
        return false;
    }

    public static int ObjectsHashCode(Object... objects) {
        return Arrays.hashCode(objects);
    }

    @Override
    public int hashCode() {
        return ObjectsHashCode(numHashFunctions, funnel, strategy, bits);
    }




    public static <T> BloomFilter<T> create(
            Funnel<? super T> funnel, int expectedInsertions, double fpp) {
        return create(funnel, (long) expectedInsertions, fpp);
    }


    public static <T> BloomFilter<T> create(
            Funnel<? super T> funnel, long expectedInsertions, double fpp) {
        return create(funnel, expectedInsertions, fpp, BloomFilterStrategies.MURMUR128_MITZ_64);
    }

    static long optimalNumOfBits(long n, double p) {
        if (p == 0) {
            p = Double.MIN_VALUE;
        }
        return (long) (-n * Math.log(p) / (Math.log(2) * Math.log(2)));
    }

    static int optimalNumOfHashFunctions(long n, long m) {
        // (m / n) * log(2), but avoid truncation due to division!
        return Math.max(1, (int) Math.round((double) m / n * Math.log(2)));
    }

    public static <T> BloomFilter<T> createByFile(
            Funnel<? super T> funnel,String filename, long expectedInsertions){
        return createByFile(funnel, filename,expectedInsertions, 0.03, BloomFilterStrategies.MURMUR128_MITZ_64);

    }

    public static <T> BloomFilter<T> createByFile(
            Funnel<? super T> funnel,String filename, long expectedInsertions, double fpp, Strategy strategy)
    {
        if (expectedInsertions == 0) {
            expectedInsertions = 1;
        }

        long numBits = optimalNumOfBits(expectedInsertions, fpp);
        int numHashFunctions = optimalNumOfHashFunctions(expectedInsertions, numBits);
        try {
            return new BloomFilter<T>(new BloomFilterStrategies.FileBitArray(numBits,filename), numHashFunctions, funnel, strategy);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Could not create FileBitArray of " + numBits + " bits", e);
        }
    }


    static <T> BloomFilter<T> create(
            Funnel<? super T> funnel, long expectedInsertions, double fpp, Strategy strategy) {
//    checkNotNull(funnel);
//    checkArgument(
//        expectedInsertions >= 0, "Expected insertions (%s) must be >= 0", expectedInsertions);
//    checkArgument(fpp > 0.0, "False positive probability (%s) must be > 0.0", fpp);
//    checkArgument(fpp < 1.0, "False positive probability (%s) must be < 1.0", fpp);
//    checkNotNull(strategy);

        if (expectedInsertions == 0) {
            expectedInsertions = 1;
        }
        /*
         * TODO(user): Put a warning in the javadoc about tiny fpp values, since the resulting size
         * is proportional to -log(p), but there is not much of a point after all, e.g.
         * optimalM(1000, 0.0000000000000001) = 76680 which is less than 10kb. Who cares!
         */
        long numBits = optimalNumOfBits(expectedInsertions, fpp);
        int numHashFunctions = optimalNumOfHashFunctions(expectedInsertions, numBits);
        try {
            return new BloomFilter<T>(new LockFreeBitArray(numBits), numHashFunctions, funnel, strategy);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Could not create BloomFilter of " + numBits + " bits", e);
        }
    }

    public static <T> BloomFilter<T> create(Funnel<? super T> funnel, int expectedInsertions) {
        return create(funnel, (long) expectedInsertions);
    }


    public static <T> BloomFilter<T> create(Funnel<? super T> funnel, long expectedInsertions) {
        return create(funnel, expectedInsertions, 0.03); // FYI, for 3%, we always get 5 hash functions
    }



    public static byte SignedBytesCheckedCast(long value) {
        byte result = (byte) value;
        //checkArgument(result == value, "Out of range: %s", value);
        return result;
    }

    public static byte UnsignedBytesCheckedCast(long value) {
        //checkArgument(value >> Byte.SIZE == 0, "out of range: %s", value);
        return (byte) value;
    }


    public static int UnsignedBytesToInt(byte value) {
        return value & 0xFF;
    }


}
