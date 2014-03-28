package com.linkedin.drelephant.math;

import com.linkedin.drelephant.analysis.Severity;

import java.util.ArrayList;
import java.util.List;

public class Statistics {

    public static final long SECOND = 1000L;
    public static final long MINUTE = 60L * SECOND;
    public static final long HOUR = 60L * MINUTE;

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

    public static long[][] findTwoGroups(long[] values) {
        long avg = average(values);
        List<Long> smaller = new ArrayList<Long>();
        List<Long> larger = new ArrayList<Long>();
        for (int i = 0; i < values.length; i++) {
            if (values[i] < avg) {
                smaller.add(values[i]);
            } else {
                larger.add(values[i]);
            }
        }

        long[][] result = new long[2][];
        result[0] = toIntArray(smaller);
        result[1] = toIntArray(larger);

        return result;
    }

    private static long[] toIntArray(List<Long> input) {
        long[] result = new long[input.size()];
        for (int i = 0; i < result.length; i++) {
            result[i] = input.get(i);
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

    public static String describeFactor(long value, long compare, String suffix) {
        return "(" + String.format("%.2f", (double) value / (double) compare) + suffix + ")";
    }

    public static String readableTimespan(long milliseconds) {
        long seconds = milliseconds / 1000;
        long minutes = seconds / 60;
        long hours = minutes / 60;
        minutes %= 60;
        seconds %= 60;
        StringBuilder sb = new StringBuilder();
        if (hours > 0) {
            sb.append(hours).append("hr ");
        }
        if (minutes > 0) {
            sb.append(minutes).append("min ");
        }
        if (seconds > 0) {
            sb.append(seconds).append("sec ");
        }
        return sb.toString().trim();
    }

    public static Severity getNumTasksSeverity(long numTasks) {
        return Severity.getSeverityAscending(numTasks,
                10, 50, 100, 200);
    }
}
