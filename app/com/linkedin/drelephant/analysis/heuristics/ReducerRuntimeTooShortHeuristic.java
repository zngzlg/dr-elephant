package com.linkedin.drelephant.analysis.heuristics;

import com.linkedin.drelephant.analysis.HeuristicResult;
import com.linkedin.drelephant.hadoop.HadoopCounterHolder;
import com.linkedin.drelephant.hadoop.HadoopJobData;
import com.linkedin.drelephant.hadoop.HadoopTaskData;

public class ReducerRuntimeTooShortHeuristic extends GenericReducerTimeHeuristic {
    private static final String message = HeuristicResult.addPossibleReducerResult("Short-lived tasks");

    public ReducerRuntimeTooShortHeuristic() {
        super(message);
    }

    @Override
    protected boolean checkAverage(long average) {
        return average < 60 * 1000;
    }

    @Override
    protected boolean skipSmallNumberOfTasks() {
        return true;
    }
}
