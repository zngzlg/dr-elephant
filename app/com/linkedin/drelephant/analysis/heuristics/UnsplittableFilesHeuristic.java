package com.linkedin.drelephant.analysis.heuristics;

import com.linkedin.drelephant.analysis.Constants;
import com.linkedin.drelephant.analysis.HeuristicResult;
import org.apache.commons.io.FileUtils;

public class UnsplittableFilesHeuristic extends GenericFileSizeHeuristic {
    public UnsplittableFilesHeuristic() {
        super("Unsplittable files at mapper side");
    }

    @Override
    protected boolean checkAverage(long average) {
        long limit = (long) ((double) Constants.HDFS_BLOCK_SIZE * 2D);
        return average > limit;
    }
}