package com.linkedin.drelephant.math;

import java.util.ArrayList;
import java.util.List;

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
    public static int[] deviates(long[] values, long buffer, double factor) {
        if (values == null || values.length == 0) {
            return new int[0];
        }

        long avg = average(values);

        //Find deviated elements

        long minimumDiff = Math.max(buffer, (long) (avg * factor));
        List<Integer> deviatedElements = new ArrayList<Integer>();

        for (int i = 0; i < values.length; i++) {
            long diff = values[i] - avg;
            if (diff > minimumDiff) {
                deviatedElements.add(i);
            }
        }

        int[] result = new int[deviatedElements.size()];
        for (int i = 0; i < result.length; i++) {
            result[i] = deviatedElements.get(i);
        }

        return result;
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
