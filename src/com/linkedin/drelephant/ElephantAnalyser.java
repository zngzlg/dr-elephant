package com.linkedin.drelephant;

import com.linkedin.drelephant.analysis.Heuristic;
import com.linkedin.drelephant.analysis.HeuristicResult;
import com.linkedin.drelephant.analysis.heuristics.*;
import com.linkedin.drelephant.hadoop.HadoopJobData;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.List;

public class ElephantAnalyser {
    private static final Logger logger = Logger.getLogger(ElephantAnalyser.class);

    List<Heuristic> heuristics = new ArrayList<Heuristic>();

    public ElephantAnalyser() {
        heuristics.add(new MapperDataSkewHeuristic());
        heuristics.add(new ReducerDataSkewHeuristic());
        heuristics.add(new UnsplittableFilesHeuristic());
        heuristics.add(new SmallFilesHeuristic());
        heuristics.add(new GeneralMapperSlowHeuristic());
        heuristics.add(new SlowShuffleSortHeuristic());
    }

    public HeuristicResult analyse(HadoopJobData data) {
        if(data.getMapperData().length == 0 && data.getReducerData().length == 0) {
            return new HeuristicResult("No mapper/reducer data received", false);
        }

        HeuristicResult result = null;
        for (Heuristic heuristic : heuristics) {
            //Apply each heuristic
            result = heuristic.apply(data);
            //Cannot continue, return the response
            if (!result.succeeded()) {
                break;
            }
        }
        return result;
    }
}
