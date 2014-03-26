package com.linkedin.drelephant.analysis.heuristics;

import com.linkedin.drelephant.analysis.Constants;
import com.linkedin.drelephant.analysis.Heuristic;
import com.linkedin.drelephant.analysis.HeuristicResult;
import com.linkedin.drelephant.hadoop.HadoopCounterHolder;
import com.linkedin.drelephant.hadoop.HadoopJobData;
import com.linkedin.drelephant.hadoop.HadoopTaskData;
import com.linkedin.drelephant.math.Statistics;
import org.apache.commons.io.FileUtils;

public abstract class GenericReducerTimeHeuristic implements Heuristic {
    private String failMessage;

    protected GenericReducerTimeHeuristic(String failMessage) {
        this.failMessage = failMessage;
    }

    /**
     * Checks if the average is out of ordinary
     *
     * @param average
     * @return true if the average is bad/problematic
     */
    protected abstract boolean checkAverage(long average);

    /**
     * Checks whether we can skip this check if there are less than 5 tasks
     *
     * @return true if we can skip the check
     */
    protected abstract boolean skipSmallNumberOfTasks();

    @Override
    public HeuristicResult apply(HadoopJobData data) {
        HadoopTaskData[] tasks = data.getReducerData();

        //Ignore tasks with little amount
        if (skipSmallNumberOfTasks() && tasks.length <= 5) {
            return HeuristicResult.SUCCESS;
        }

        //Gather data
        long[] runTimes = new long[tasks.length];

        for (int i = 0; i < tasks.length; i++) {
            runTimes[i] = tasks[i].getRunTime();
        }

        //Analyze data
        long average = Statistics.average(runTimes);

        if (checkAverage(average)) {
            HeuristicResult result = new HeuristicResult(failMessage, false);

            result.addDetail("Number of tasks", Integer.toString(tasks.length));
            result.addDetail("Average task time", Statistics.readableTimespan(average));

            return result;
        }

        return HeuristicResult.SUCCESS;
    }
}
