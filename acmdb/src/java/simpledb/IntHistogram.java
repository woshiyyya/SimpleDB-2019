package simpledb;

import java.util.ArrayList;
import simpledb.Predicate.Op;

/**
 * A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {

    /**
     * Create a new IntHistogram.
     * <p>
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * <p>
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * <p>
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't
     * simply store every value that you see in a sorted list.
     *
     * @param buckets The number of buckets to split the input value into.
     * @param min The minimum integer value that will ever be passed to this class for histogramming
     * @param max The maximum integer value that will ever be passed to this class for histogramming
     */

    private int[] Histogram;
    private int[] binLeft;
    private int[] binRight;
    private int min;
    private int max;
    private int bin;
    private int buckets;
    private int ntups;

    public IntHistogram(int buckets, int min, int max) {
        // some code goes here
        this.min = min;
        this.max = max;
        this.buckets = buckets;
        this.bin = (int) Math.ceil((1.0 * (max - min + 1)) / buckets);

        Histogram = new int[buckets];
        binRight = new int[buckets];
        binLeft = new int[buckets];

        for (int i = 0; i < buckets; i++) {
            Histogram[i] = 0;
            binLeft[i] = i * bin + min;
            binRight[i] = (i + 1) * bin + min;
        }
    }

    private int indexing(int v){
        return (v - min) / bin;
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     *
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
        // some code goes here
        Histogram[(v - min) / bin] += 1;
        ntups++;
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * <p>
     * For example, if "op" is "GREATER_THAN" and "v" is 5,
     * return your estimate of the fraction of elements that are greater than 5.
     *
     * @param op Operator
     * @param v  Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {

        // some code goes here
        switch (op){
            case EQUALS:
                if (v < min || v > max) return 0.0;
                return calculateSelectivity(Op.EQUALS, v);
            case NOT_EQUALS:
                if (v < min || v > max) return 1.0;
                return 1 - calculateSelectivity(Op.EQUALS, v);
            case LESS_THAN:
                if (v <= min) return 0.0;
                if (v > max) return 1.0;
                return calculateSelectivity(Op.LESS_THAN, v);
            case GREATER_THAN:
                if (v < min) return 1.0;
                if (v >= max) return 0.0;
                return calculateSelectivity(Op.GREATER_THAN, v);
            case LESS_THAN_OR_EQ:
                if (v < min) return 0.0;
                if (v >= max) return 1.0;
                return calculateSelectivity(Op.EQUALS, v) + calculateSelectivity(Op.LESS_THAN, v);
            case GREATER_THAN_OR_EQ:
                if (v <= min) return 1.0;
                if (v > max) return 0.0;
                return calculateSelectivity(Op.EQUALS, v) + calculateSelectivity(Op.GREATER_THAN, v);
            case LIKE:
                return 1.0;
        }
        return 0;
    }

    private double calculateSelectivity(Predicate.Op op, int v){
        if (op == Op.EQUALS){
            return 1.0 * Histogram[indexing(v)] / (bin * ntups);
        }
        if (op == Op.GREATER_THAN){
            double selectivity = (1.0 * Histogram[indexing(v)] / ntups) * (binRight[indexing(v)] - v) / bin;
            for (int i = indexing(v) + 1; i < buckets; i++){
                selectivity += (1.0 * Histogram[i] / ntups);
            }
            return selectivity;
        }
        if (op == Op.LESS_THAN){
            double selectivity = (1.0 * Histogram[indexing(v)] / ntups) * (v - binLeft[indexing(v)]) / bin;
            for (int i = indexing(v) - 1; i >= 0; i--){
                selectivity += (1.0 * Histogram[i]/ ntups);
            }
            return selectivity;
        }
        return 0;
    }

    /**
     * @return the average selectivity of this histogram.
     * <p>
     * This is not an indispensable method to implement the basic
     * join optimization. It may be needed if you want to
     * implement a more efficient optimization
     */
    public double avgSelectivity() {
        // some code goes here
        return 1.0;
    }

    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        // some code goes here
        return null;
    }
}
