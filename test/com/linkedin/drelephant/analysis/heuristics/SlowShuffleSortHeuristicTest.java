package com.linkedin.drelephant.analysis.heuristics;

import com.linkedin.drelephant.analysis.Heuristic;
import com.linkedin.drelephant.analysis.HeuristicResult;
import com.linkedin.drelephant.hadoop.HadoopCounterHolder;
import com.linkedin.drelephant.hadoop.HadoopJobData;
import com.linkedin.drelephant.hadoop.HadoopTaskData;
import junit.framework.TestCase;
import org.apache.hadoop.mapred.TaskID;

public class SlowShuffleSortHeuristicTest extends TestCase {
    Heuristic heuristic = new SlowShuffleSortHeuristic();

    public void testApplyFailShuffle() throws Exception {
        HadoopCounterHolder counters = new HadoopCounterHolder();
        HadoopTaskData[] mappers = new HadoopTaskData[3];
        HadoopTaskData[] reducers = new HadoopTaskData[3];

        HadoopCounterHolder counter_a = new HadoopCounterHolder();
        HadoopCounterHolder counter_b = new HadoopCounterHolder();
        HadoopCounterHolder counter_c = new HadoopCounterHolder();

        mappers[0] = new HadoopTaskData(counter_a, 0, 1000, TaskID.forName("task_201402260232_111111_r_000000"));
        mappers[1] = new HadoopTaskData(counter_b, 0, 1000, TaskID.forName("task_201402260232_111112_r_000000"));
        mappers[2] = new HadoopTaskData(counter_c, 0, 1000, TaskID.forName("task_201402260232_111112_r_000000"));

        reducers[0] = new HadoopTaskData(counters, 0, 60 * 1000, TaskID.forName("task_201402260232_111113_r_000000"));
        reducers[1] = new HadoopTaskData(counters, 0, 60 * 1000, TaskID.forName("task_201402260232_111114_r_000000"));
        reducers[2] = new HadoopTaskData(counters, 0, 60 * 1000, TaskID.forName("task_201402260232_111114_r_000000"));

        reducers[0].setShuffleTime(60 * 5000);
        reducers[1].setShuffleTime(60 * 5000);
        reducers[2].setShuffleTime(60 * 5000);

        reducers[0].setSortTime(0);
        reducers[1].setSortTime(0);
        reducers[2].setSortTime(0);

        HadoopJobData data = new HadoopJobData(counters, mappers, reducers);

        HeuristicResult result = heuristic.apply(data);

        assertFalse(result.succeeded());
    }

    public void testApplyFailSort() throws Exception {
        HadoopCounterHolder counters = new HadoopCounterHolder();
        HadoopTaskData[] mappers = new HadoopTaskData[3];
        HadoopTaskData[] reducers = new HadoopTaskData[3];

        HadoopCounterHolder counter_a = new HadoopCounterHolder();
        HadoopCounterHolder counter_b = new HadoopCounterHolder();
        HadoopCounterHolder counter_c = new HadoopCounterHolder();

        mappers[0] = new HadoopTaskData(counter_a, 0, 1000, TaskID.forName("task_201402260232_111111_r_000000"));
        mappers[1] = new HadoopTaskData(counter_b, 0, 1000, TaskID.forName("task_201402260232_111112_r_000000"));
        mappers[2] = new HadoopTaskData(counter_c, 0, 1000, TaskID.forName("task_201402260232_111112_r_000000"));

        reducers[0] = new HadoopTaskData(counters, 0, 60 * 1000, TaskID.forName("task_201402260232_111113_r_000000"));
        reducers[1] = new HadoopTaskData(counters, 0, 60 * 1000, TaskID.forName("task_201402260232_111114_r_000000"));
        reducers[2] = new HadoopTaskData(counters, 0, 60 * 1000, TaskID.forName("task_201402260232_111114_r_000000"));

        reducers[0].setShuffleTime(0);
        reducers[1].setShuffleTime(0);
        reducers[2].setShuffleTime(0);

        reducers[0].setSortTime(60 * 5000);
        reducers[1].setSortTime(60 * 5000);
        reducers[2].setSortTime(60 * 5000);

        HadoopJobData data = new HadoopJobData(counters, mappers, reducers);

        HeuristicResult result = heuristic.apply(data);

        assertFalse(result.succeeded());
    }

    public void testApplySuccess() throws Exception {
        HadoopCounterHolder counters = new HadoopCounterHolder();
        HadoopTaskData[] mappers = new HadoopTaskData[3];
        HadoopTaskData[] reducers = new HadoopTaskData[3];

        HadoopCounterHolder counter_a = new HadoopCounterHolder();
        HadoopCounterHolder counter_b = new HadoopCounterHolder();
        HadoopCounterHolder counter_c = new HadoopCounterHolder();

        mappers[0] = new HadoopTaskData(counter_a, 0, 1000, TaskID.forName("task_201402260232_111111_r_000000"));
        mappers[1] = new HadoopTaskData(counter_b, 0, 1000, TaskID.forName("task_201402260232_111112_r_000000"));
        mappers[2] = new HadoopTaskData(counter_c, 0, 1000, TaskID.forName("task_201402260232_111112_r_000000"));

        reducers[0] = new HadoopTaskData(counters, 0, 60 * 1000, TaskID.forName("task_201402260232_111113_r_000000"));
        reducers[1] = new HadoopTaskData(counters, 0, 60 * 1000, TaskID.forName("task_201402260232_111114_r_000000"));
        reducers[2] = new HadoopTaskData(counters, 0, 60 * 1000, TaskID.forName("task_201402260232_111114_r_000000"));

        reducers[0].setShuffleTime(0);
        reducers[1].setShuffleTime(0);
        reducers[2].setShuffleTime(0);

        reducers[0].setSortTime(0);
        reducers[1].setSortTime(0);
        reducers[2].setSortTime(0);

        HadoopJobData data = new HadoopJobData(counters, mappers, reducers);

        HeuristicResult result = heuristic.apply(data);

        assertTrue(result.succeeded());
    }
}
