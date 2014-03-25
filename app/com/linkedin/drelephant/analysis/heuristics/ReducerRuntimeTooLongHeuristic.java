package com.linkedin.drelephant.analysis.heuristics;

import com.linkedin.drelephant.analysis.HeuristicResult;

public class ReducerRuntimeTooLongHeuristic extends GenericReducerTimeHeuristic {
    private static final String message = HeuristicResult.addPossibleReducerResult("Tasks taking a long time");

    public ReducerRuntimeTooLongHeuristic() {
        super(message);
    }

    @Override
    protected boolean checkAverage(long average) {
        return average > 60 * 60 * 1000;
    }

    @Override
    protected boolean skipSmallNumberOfTasks() {
        return false;
    }
}
