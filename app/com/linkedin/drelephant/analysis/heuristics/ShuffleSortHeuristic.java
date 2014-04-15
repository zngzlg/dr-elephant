package com.linkedin.drelephant.analysis.heuristics;

import com.linkedin.drelephant.analysis.Constants;
import com.linkedin.drelephant.analysis.Heuristic;
import com.linkedin.drelephant.analysis.HeuristicResult;
import com.linkedin.drelephant.analysis.Severity;
import com.linkedin.drelephant.hadoop.HadoopJobData;
import com.linkedin.drelephant.hadoop.HadoopTaskData;
import com.linkedin.drelephant.math.Statistics;

public class ShuffleSortHeuristic implements Heuristic {
    public static final String heuristicName = "Shuffle & Sort";

    @Override
    public String getHeuristicName() {
        return heuristicName;
    }

    @Override
    public HeuristicResult apply(HadoopJobData data) {
        HadoopTaskData[] tasks = Statistics.createSample(HadoopTaskData.class, data.getReducerData(), Constants.SHUFFLE_SORT_MAX_SAMPLE_SIZE);

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

        HeuristicResult result = new HeuristicResult(heuristicName, severity);

        result.addDetail("Number of tasks", Integer.toString(data.getReducerData().length));
        result.addDetail("Average code runtime", Statistics.readableTimespan(avgExecTime));
        String shuffleFactor = Statistics.describeFactor(avgShuffleTime, avgExecTime, "x");
        result.addDetail("Average shuffle time", Statistics.readableTimespan(avgShuffleTime) + " " + shuffleFactor);
        String sortFactor = Statistics.describeFactor(avgSortTime, avgExecTime, "x");
        result.addDetail("Average sort time", Statistics.readableTimespan(avgSortTime) + " " + sortFactor);

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
                10 * Statistics.MINUTE,
                30 * Statistics.MINUTE);

        if (codetime <= 0) {
            return runtimeSeverity;
        }
        long value = runtime * 2 / codetime;
        Severity runtimeRatioSeverity = Severity.getSeverityAscending(value,
                1, 2, 4, 8);

        return Severity.min(runtimeSeverity, runtimeRatioSeverity);
    }
}