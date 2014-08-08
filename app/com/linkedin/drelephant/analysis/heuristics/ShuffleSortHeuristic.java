package com.linkedin.drelephant.analysis.heuristics;

import java.util.ArrayList;
import java.util.List;

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

        HadoopTaskData[] tasks = data.getReducerData();

        List<Long> execTime = new ArrayList<Long>();
        List<Long> shuffleTime = new ArrayList<Long>();
        List<Long> sortTime = new ArrayList<Long>();

        for(HadoopTaskData task : tasks) {
          if(task.timed()) {
            execTime.add(task.getExecutionTime());
            shuffleTime.add(task.getShuffleTime());
            sortTime.add(task.getSortTime());
          }
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