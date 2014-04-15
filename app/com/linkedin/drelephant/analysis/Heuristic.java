package com.linkedin.drelephant.analysis;

import com.linkedin.drelephant.hadoop.HadoopJobData;

public interface Heuristic {
    public HeuristicResult apply(HadoopJobData data);

    public String getHeuristicName();
}
