package com.linkedin.drelephant.analysis.heuristics;

import com.linkedin.drelephant.analysis.Heuristic;
import com.linkedin.drelephant.analysis.HeuristicResult;
import com.linkedin.drelephant.analysis.Severity;
import com.linkedin.drelephant.hadoop.HadoopJobData;
import com.linkedin.drelephant.hadoop.HadoopTaskData;
import com.linkedin.drelephant.math.Statistics;

public class ReducerTimeHeuristic implements Heuristic {
    private static final String heuristicName = "Reducer Time";

    @Override
    public String getHeuristicName() {
        return heuristicName;
    }

    @Override
    public HeuristicResult apply(HadoopJobData data) {
        HadoopTaskData[] tasks = data.getReducerData();

        //Gather data
        long[] runTimes = new long[tasks.length];

        for (int i = 0; i < tasks.length; i++) {
            runTimes[i] = tasks[i].getRunTime();
        }

        //Analyze data
        long averageRuntime = Statistics.average(runTimes);

        Severity shortTimeSeverity = shortTimeSeverity(averageRuntime, tasks.length);
        Severity longTimeSeverity = longTimeSeverity(averageRuntime, tasks.length);
        Severity severity = Severity.max(shortTimeSeverity, longTimeSeverity);

        HeuristicResult result = new HeuristicResult(heuristicName, severity);

        result.addDetail("Number of tasks", Integer.toString(tasks.length));
        result.addDetail("Average task time", Statistics.readableTimespan(averageRuntime));

        return result;
    }

    private Severity shortTimeSeverity(long runtime, long numTasks) {
        Severity timeSeverity = getShortRuntimeSeverity(runtime);
        Severity taskSeverity = Statistics.getNumTasksSeverity(numTasks);
        return Severity.min(timeSeverity, taskSeverity);
    }

    private Severity longTimeSeverity(long runtime, long numTasks) {
        Severity timeSeverity = getLongRuntimeSeverity(runtime);
        Severity taskSeverity = getNumTasksSeverityReverse(numTasks);
        return Severity.min(timeSeverity, taskSeverity);
    }

    public static Severity getShortRuntimeSeverity(long runtime) {
        return Severity.getSeverityDescending(runtime,
                10 * Statistics.MINUTE,
                5 * Statistics.MINUTE,
                2 * Statistics.MINUTE,
                1 * Statistics.MINUTE);
    }

    public static Severity getLongRuntimeSeverity(long runtime) {
        return Severity.getSeverityAscending(runtime,
                30 * Statistics.MINUTE,
                1 * Statistics.HOUR,
                2 * Statistics.HOUR,
                5 * Statistics.HOUR);
    }

    public static Severity getNumTasksSeverityReverse(long numTasks) {
        return Severity.getSeverityDescending(numTasks,
                50, 20, 10, 5);
    }
}
