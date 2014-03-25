package com.linkedin.drelephant.analysis.heuristics;

import com.linkedin.drelephant.analysis.Constants;
import com.linkedin.drelephant.analysis.HeuristicResult;

public class UnsplittableFilesHeuristic extends GenericFileSizeHeuristic {
    private static final String message = HeuristicResult.addPossibleMapperResult("Unsplittable files");

    public UnsplittableFilesHeuristic() {
        super(message);
    }

    @Override
    protected boolean checkAverage(long average) {
        long limit = (long) ((double) Constants.HDFS_BLOCK_SIZE * 2D);
        return average > limit;
    }
}