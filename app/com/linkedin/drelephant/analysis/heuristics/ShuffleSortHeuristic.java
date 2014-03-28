package com.linkedin.drelephant.analysis.heuristics;

import com.linkedin.drelephant.analysis.Constants;
import com.linkedin.drelephant.analysis.Heuristic;
import com.linkedin.drelephant.analysis.HeuristicResult;
import com.linkedin.drelephant.analysis.Severity;
import com.linkedin.drelephant.hadoop.HadoopJobData;
import com.linkedin.drelephant.hadoop.HadoopTaskData;
import com.linkedin.drelephant.math.Statistics;

import java.util.Arrays;
import java.util.Collections;

public class ShuffleSortHeuristic implements Heuristic {
    private static final String analysisName = "Shuffle & Sort";

    @Override
    public HeuristicResult apply(HadoopJobData data) {
        HadoopTaskData[] tasks = createSample(data.getReducerData());

        //Gather data
        fetchShuffleSort(tasks);

        long[] execTime = new long[tasks.length];
        long[] shuffleTime = new long[tasks.length];
        long[] sortTime = new long[tasks.length];

        for (int i = 0; i < tasks.length; i++) {
            execTime[i] = tasks[i].getExecutionTime();
            shuffleTime[i] = tasks[i].getShuffleTime();
            sortTime[i] = tasks[i].getSortTime();
        }

        //Analyze data
        long avgExecTime = Statistics.average(execTime);
        long avgShuffleTime = Statistics.average(shuffleTime);
        long avgSortTime = Statistics.average(sortTime);


        Severity shuffleSeverity = getShuffleSortSeverity(avgShuffleTime, avgExecTime);
        Severity sortSeverity = getShuffleSortSeverity(avgSortTime, avgExecTime);
        Severity severity = Severity.max(shuffleSeverity, sortSeverity);

        HeuristicResult result = new HeuristicResult(analysisName, severity);

        result.addDetail("Number of tasks", Integer.toString(data.getReducerData().length));
        result.addDetail("Average code runtime", Statistics.readableTimespan(avgExecTime));
        String shuffleFactor = Statistics.describeFactor(avgShuffleTime, avgExecTime, "x");
        result.addDetail("Average shuffle time", Statistics.readableTimespan(avgShuffleTime) + " " + shuffleFactor);
        String sortFactor = Statistics.describeFactor(avgSortTime, avgExecTime, "x");
        result.addDetail("Average sort time", Statistics.readableTimespan(avgSortTime) + " " + sortFactor);

        return result;
    }

    private HadoopTaskData[] createSample(HadoopTaskData[] reducers) {
        int MAX_SAMPLE_SIZE = Constants.SHUFFLE_SORT_MAX_SAMPLE_SIZE;

        //Skip this process if number of items already smaller than sample size
        if (reducers.length <= MAX_SAMPLE_SIZE) {
            return reducers;
        }

        HadoopTaskData[] result = new HadoopTaskData[MAX_SAMPLE_SIZE];

        //Shuffle a clone copy
        HadoopTaskData[] clone = reducers.clone();
        Collections.shuffle(Arrays.asList(clone));

        //Take the first n items
        System.arraycopy(clone, 0, result, 0, MAX_SAMPLE_SIZE);

        return result;
    }

    private void fetchShuffleSort(HadoopTaskData[] reducers) {
        for (HadoopTaskData reducer : reducers) {
            reducer.fetchTaskDetails();
        }
    }

    public static Severity getShuffleSortSeverity(long runtime, long codetime) {
        Severity runtimeSeverity = Severity.getSeverityAscending(runtime,
                1 * Statistics.MINUTE,
                5 * Statistics.MINUTE,
                20 * Statistics.MINUTE,
                1 * Statistics.HOUR);

        if (codetime <= 0) {
            return runtimeSeverity;
        }
        long value = runtime * 2 / codetime;
        Severity runtimeRatioSeverity = Severity.getSeverityAscending(value,
                1, 2, 4, 8);

        return Severity.min(runtimeSeverity, runtimeRatioSeverity);
    }
}