package com.linkedin.drelephant.analysis.heuristics;

import com.linkedin.drelephant.analysis.Constants;
import com.linkedin.drelephant.analysis.HeuristicResult;
import org.apache.commons.io.FileUtils;

public class SmallFilesHeuristic extends GenericFileSizeHeuristic {
    public SmallFilesHeuristic() {
        super("Small files at mapper side");
    }

    @Override
    protected boolean checkAverage(long average) {
        long limit = Constants.HDFS_BLOCK_SIZE / 4;
        return average < limit;
    }
}
