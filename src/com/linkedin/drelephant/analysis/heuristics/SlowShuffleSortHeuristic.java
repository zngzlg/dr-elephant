package com.linkedin.drelephant.analysis.heuristics;

import com.linkedin.drelephant.analysis.Heuristic;
import com.linkedin.drelephant.analysis.HeuristicResult;
import com.linkedin.drelephant.hadoop.HadoopJobData;
import com.linkedin.drelephant.hadoop.HadoopTaskData;
import com.linkedin.drelephant.math.Statistics;

import java.util.Arrays;
import java.util.Collections;

public class SlowShuffleSortHeuristic implements Heuristic {
    private static final int MAX_SAMPLE_SIZE = 50;

    @Override
    public HeuristicResult apply(HadoopJobData data) {
        HadoopTaskData[] reducers = createSample(data.getReducerData());

        fetchShuffleSort(reducers);

        long[] execTime = new long[reducers.length];
        long[] shuffleTime = new long[reducers.length];
        long[] sortTime = new long[reducers.length];

        for (int i = 0; i < reducers.length; i++) {
            execTime[i] = reducers[i].getExecutionTime();
            shuffleTime[i] = reducers[i].getShuffleTime();
            sortTime[i] = reducers[i].getSortTime();
        }

        long avgExecTime = Statistics.average(execTime);
        long avgShuffleTime = Statistics.average(shuffleTime);
        long avgSortTime = Statistics.average(sortTime);

        // Has to be at least a minute to be significant
        long limit = Math.max(avgExecTime, 60 * 1000);

        if (avgShuffleTime > limit) {
            return buildShuffleError(avgShuffleTime, avgExecTime);
        }

        if (avgSortTime > limit) {
            return buildSortError(avgSortTime, avgExecTime);
        }

        return HeuristicResult.SUCCESS;
    }

    private HeuristicResult buildShuffleError(long avg, long avgExec) {
        String error = "Slow shuffle in reducers. \nAverage shuffle time (ms): " + avg +
                "\nAverage code runtime (ms): " + avgExec;
        return new HeuristicResult(error, false);
    }

    private HeuristicResult buildSortError(long avg, long avgExec) {
        String error = "Slow sort in reducers. \nAverage sort time (ms): " + avg +
                "\nAverage code runtime (ms): " + avgExec;
        return new HeuristicResult(error, false);
    }

    private HadoopTaskData[] createSample(HadoopTaskData[] reducers) {
        if (reducers.length <= MAX_SAMPLE_SIZE) {
            return reducers;
        }
        HadoopTaskData[] clone = reducers.clone();

        HadoopTaskData[] result = new HadoopTaskData[MAX_SAMPLE_SIZE];
        Collections.shuffle(Arrays.asList(clone));

        System.arraycopy(clone, 0, result, 0, MAX_SAMPLE_SIZE);

        return result;
    }

    private void fetchShuffleSort(HadoopTaskData[] reducers) {
        for (HadoopTaskData reducer : reducers) {
            reducer.fetchTaskDetails();
        }
    }
}