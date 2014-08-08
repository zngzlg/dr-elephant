package com.linkedin.drelephant.analysis.heuristics;

import java.util.ArrayList;
import java.util.List;

import com.linkedin.drelephant.analysis.Heuristic;
import com.linkedin.drelephant.analysis.HeuristicResult;
import com.linkedin.drelephant.analysis.Severity;
import com.linkedin.drelephant.hadoop.HadoopJobData;
import com.linkedin.drelephant.hadoop.HadoopTaskData;
import com.linkedin.drelephant.math.Statistics;

public class ReducerTimeHeuristic implements Heuristic {
    public static final String heuristicName = "Reducer Time";

    @Override
    public String getHeuristicName() {
        return heuristicName;
    }

    @Override
    public HeuristicResult apply(HadoopJobData data) {
        HadoopTaskData[] tasks = data.getReducerData();

        List<Long> runTimes = new ArrayList<Long>();

        for(HadoopTaskData task : tasks) {
          if(task.timed()) {
            runTimes.add(task.getRunTime());
          }
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
        Severity taskSeverity = getNumTasksSeverity(numTasks);
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
                15 * Statistics.MINUTE,
                30 * Statistics.MINUTE,
                1 * Statistics.HOUR,
                2 * Statistics.HOUR);
    }

    public static Severity getNumTasksSeverity(long numTasks) {
        return Severity.getSeverityAscending(numTasks,
                10, 50, 200, 500);
    }

    public static Severity getNumTasksSeverityReverse(long numTasks) {
        return Severity.getSeverityDescending(numTasks,
                100, 50, 20, 10);
    }
}
