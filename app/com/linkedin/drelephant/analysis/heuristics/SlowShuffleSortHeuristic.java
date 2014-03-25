package com.linkedin.drelephant.analysis.heuristics;

import com.linkedin.drelephant.analysis.Constants;
import com.linkedin.drelephant.analysis.Heuristic;
import com.linkedin.drelephant.analysis.HeuristicResult;
import com.linkedin.drelephant.hadoop.HadoopJobData;
import com.linkedin.drelephant.hadoop.HadoopTaskData;
import com.linkedin.drelephant.math.Statistics;

import java.util.Arrays;
import java.util.Collections;

public class SlowShuffleSortHeuristic implements Heuristic {
    private static final String messageShuffle = HeuristicResult.addPossibleReducerResult("Slow shuffle");
    private static final String messageSort = HeuristicResult.addPossibleReducerResult("Slow sort");
    private static final String messageBoth = HeuristicResult.addPossibleReducerResult("Slow shuffle & sort");

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

        // Has to be at least a minute to be significant
        long limit = Math.max(avgExecTime, 60 * 1000);

        boolean slowShuffle = avgShuffleTime > limit;
        boolean slowSort = avgSortTime > limit;

        if (!slowShuffle && !slowSort) {
            return HeuristicResult.SUCCESS;
        }

        String failMessage = "";
        if (slowShuffle && slowSort) {
            failMessage = messageBoth;
        } else if (slowShuffle) {
            failMessage = messageShuffle;
        } else {
            failMessage = messageSort;
        }


        HeuristicResult result = new HeuristicResult(failMessage, false);

        result.addDetail("Number of tasks", Integer.toString(tasks.length));
        result.addDetail("Average code runtime", Statistics.readableTimespan(avgExecTime));

        if (slowShuffle) {
            String deviationFactor = Statistics.describeFactor(avgShuffleTime, avgExecTime, "x");
            result.addDetail("Average shuffle time", Statistics.readableTimespan(avgShuffleTime) + " " + deviationFactor);
        }

        if (slowSort) {
            String deviationFactor = Statistics.describeFactor(avgSortTime, avgExecTime, "x");
            result.addDetail("Average sort time", Statistics.readableTimespan(avgSortTime) + " " + deviationFactor);
        }

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
}