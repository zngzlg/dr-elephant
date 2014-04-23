package com.linkedin.drelephant;

import com.linkedin.drelephant.analysis.Heuristic;
import com.linkedin.drelephant.analysis.HeuristicResult;
import com.linkedin.drelephant.analysis.Severity;
import com.linkedin.drelephant.analysis.heuristics.*;
import com.linkedin.drelephant.hadoop.HadoopJobData;
import model.JobType;
import org.apache.log4j.Logger;

import java.util.*;

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

    public JobType getJobType(HadoopJobData data) {
        String pigVersion = data.getJobConf().getProperty("pig.version");
        if (pigVersion != null && !pigVersion.isEmpty()) {
            return JobType.PIG;
        }
        String hiveMapredMode = data.getJobConf().getProperty("hive.mapred.mode");
        if (hiveMapredMode != null && !hiveMapredMode.isEmpty()) {
            return JobType.HIVE;
        }

        return JobType.HADOOPJAVA;
    }


    public Map<String, String> getMetaUrls(HadoopJobData data) {
        Map<String, String> result = new HashMap<String, String>();
        final String prefix = "meta.url.";
        Properties jobConf = data.getJobConf();
        for (Map.Entry<Object, Object> entry : jobConf.entrySet()) {
            if (entry.getKey().toString().startsWith(prefix)) {
                String key = entry.getKey().toString();
                String value = jobConf.getProperty(key);
                result.put(key.substring(prefix.length()), value);
            }
        }
        return result;
    }

    public static ElephantAnalyser instance() {
        return instance;
    }
}
