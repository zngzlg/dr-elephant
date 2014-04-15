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
    public static final String NO_DATA = "No Data Received";
    private static final Logger logger = Logger.getLogger(ElephantAnalyser.class);
    private static final ElephantAnalyser instance = new ElephantAnalyser();


    private HeuristicResult nodata;
    private List<Heuristic> heuristics = new ArrayList<Heuristic>();
    public List<String> heuristicNames = new ArrayList<String>();


    public ElephantAnalyser() {
        nodata = new HeuristicResult(NO_DATA, Severity.LOW);
        addHeuristic(new MapperDataSkewHeuristic());
        addHeuristic(new ReducerDataSkewHeuristic());
        addHeuristic(new MapperInputSizeHeuristic());
        addHeuristic(new MapperSpeedHeuristic());
        addHeuristic(new ReducerTimeHeuristic());
        addHeuristic(new ShuffleSortHeuristic());
    }

    public void addHeuristic(Heuristic heuristic) {
        heuristics.add(heuristic);
        heuristicNames.add(heuristic.getHeuristicName());
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

    public String getJobType(HadoopJobData data) {
        String pigVersion = data.getJobConf().getProperty("pig.version");
        if (pigVersion != null && !pigVersion.isEmpty()) {
            return "Pig";
        }
        String hiveMapredMode = data.getJobConf().getProperty("hive.mapred.mode");
        if (hiveMapredMode != null && !hiveMapredMode.isEmpty()) {
            return "Hive";
        }

        return "Unknown/Hadoop";
    }

    public static ElephantAnalyser instance() {
        return instance;
    }
}
