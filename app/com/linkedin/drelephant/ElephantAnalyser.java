package com.linkedin.drelephant;

import com.linkedin.drelephant.analysis.Heuristic;
import com.linkedin.drelephant.analysis.HeuristicResult;
import com.linkedin.drelephant.analysis.Severity;
import com.linkedin.drelephant.analysis.heuristics.*;
import com.linkedin.drelephant.hadoop.HadoopJobData;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.List;

public class ElephantAnalyser {
    private static final Logger logger = Logger.getLogger(ElephantAnalyser.class);
    private static final ElephantAnalyser instance = new ElephantAnalyser();

    private HeuristicResult nodata;
    private List<Heuristic> heuristics = new ArrayList<Heuristic>();


    public ElephantAnalyser() {
        nodata = new HeuristicResult("No data received", Severity.LOW);
        heuristics.add(new MapperDataSkewHeuristic());
        heuristics.add(new ReducerDataSkewHeuristic());
        heuristics.add(new MapperInputSizeHeuristic());
        heuristics.add(new MapperSpeedHeuristic());
        heuristics.add(new ReducerTimeHeuristic());
        heuristics.add(new ShuffleSortHeuristic());
    }

    public HeuristicResult[] analyse(HadoopJobData data) {
        if (data.getMapperData().length == 0 && data.getReducerData().length == 0) {
            return new HeuristicResult[]{nodata};
        }

        List<HeuristicResult> results = new ArrayList<HeuristicResult>();
        for (Heuristic heuristic : heuristics) {
            results.add(heuristic.apply(data));
        }
        return results.toArray(new HeuristicResult[results.size()]);
    }

    public static ElephantAnalyser instance() {
        return instance;
    }
}
