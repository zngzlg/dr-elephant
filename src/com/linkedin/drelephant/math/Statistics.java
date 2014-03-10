package com.linkedin.drelephant.math;

public class Statistics {

    /**
     * Check if the array has deviating elements.
     * <p/>
     * Deviating elements are found by comparing each individual value against the average.
     *
     * @param values the array of values to check
     * @param buffer the amount to ignore as a buffer for smaller valued lists
     * @param factor the amount of allowed deviation is calculated from average * factor
     * @return the index of the deviating value, or -1 if
     */
    public static int deviates(long[] values, long buffer, double factor) {
        if (values == null || values.length == 0) {
            return -1;
        }

        long avg = average(values);

        //Find most deviated element
        int mostDeviatedElement = -1;
        long mostDiff = -1;

        for (int i = 0; i < values.length; i++) {
            long diff = Math.abs(values[i] - avg);
            if (diff > mostDiff) {
                mostDeviatedElement = i;
                mostDiff = diff;
            }
        }

        long minimumDiff = Math.max(buffer, (long) (avg * factor));

        if (mostDiff > minimumDiff) {
            return mostDeviatedElement;
        }

        return -1;
    }

    public static long average(long[] values) {
        //Find average
        double sum = 0d;
        for (long value : values) {
            sum += value;
        }
        return (long) (sum / (double) values.length);
    }
}
